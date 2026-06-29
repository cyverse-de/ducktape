"""`DucktapeFileSystem`: an fsspec `AbstractFileSystem` backed by iRODS.

This is the registered `irods://` backend. It owns one `iRODSSession` (via a
`SessionProvider`) and threads it explicitly into file handles and helpers rather than
using any module-global state. Metadata operations and connection checkout are guarded
by a re-entrant lock; the actual byte transfers in the file objects are not (each open
file owns its own connection — see `file.py`).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Generator, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from fsspec.callbacks import DEFAULT_CALLBACK, Callback
from fsspec.spec import AbstractFileSystem
from irods.exception import (
    CAT_COLLECTION_NOT_EMPTY,
    CAT_NO_ROWS_FOUND,
    HIERARCHY_ERROR,
    DataObjectDoesNotExist,
    DoesNotExist,
    PycommandsException,
    iRODSException,
)

from . import listing
from .auth import DEFAULT_PORT, SessionProvider, SingleSessionProvider, resolve_auth
from .errors import IrodsFileNotFoundError, IrodsNotEmptyError, IrodsOperationError
from .file import DucktapeBufferedFile
from .listing import InfoDict
from .paths import base_name, normalize_irods_path, parent_path

if TYPE_CHECKING:
    from collections.abc import Callable

    from irods.session import iRODSSession

logger = logging.getLogger("ducktape")

DEFAULT_BLOCK_SIZE = 4 * 2**20
DEFAULT_CACHE_TYPE = "readahead"
# On iRODS >= 4.3.1, concurrent opens can intermittently fail with HIERARCHY_ERROR while the
# server resolves the resource hierarchy; the operation succeeds on a quick retry. Retries are
# on by default; set hierarchy_retries=0 to disable.
DEFAULT_HIERARCHY_RETRIES = 3
DEFAULT_HIERARCHY_RETRY_BACKOFF = 0.1


@contextmanager
def _translate_errors(path: str) -> Iterator[None]:
    """Map raw python-irodsclient exceptions to ducktape's typed errors.

    Keeps the write/copy/mkdir paths consistent with the read path so fsspec and DuckDB see
    `FileNotFoundError` for missing objects and a single `IrodsOperationError` (an `OSError`)
    otherwise, with the PRC cause preserved via `__cause__`.
    """
    try:
        yield
    except (DoesNotExist, CAT_NO_ROWS_FOUND) as exc:
        raise IrodsFileNotFoundError(path) from exc
    except CAT_COLLECTION_NOT_EMPTY as exc:
        raise IrodsNotEmptyError(path) from exc
    except (PycommandsException, iRODSException) as exc:
        raise IrodsOperationError(
            f"iRODS operation failed for {path!r}: {exc}"
        ) from exc


def _threadsafe_progress(callback: Callback) -> Callable[[int], None]:
    """A byte-count updater for PRC `updatables`, safe to call from transfer threads.

    PRC's parallel get/put invoke the updatable from several worker threads, and fsspec's
    `Callback.relative_update` does a non-atomic `self.value += inc`; the lock keeps the
    progress total from losing increments.
    """
    lock = threading.Lock()

    def update(num_bytes: int) -> None:
        with lock:
            callback.relative_update(num_bytes)

    return update


class DucktapeFileSystem(AbstractFileSystem):
    """fsspec filesystem for iRODS logical paths (`irods:///zone/...`)."""

    protocol = "irods"

    def __init__(
        self,
        *,
        host: str | None = None,
        port: int = DEFAULT_PORT,
        user: str | None = None,
        password: str | None = None,
        zone: str | None = None,
        irods_env_file: str | None = None,
        block_size: int = DEFAULT_BLOCK_SIZE,
        cache_type: str = DEFAULT_CACHE_TYPE,
        num_threads: int = 0,
        listing_page_size: int | None = None,
        allow_redirect: bool = False,
        hierarchy_retries: int = DEFAULT_HIERARCHY_RETRIES,
        hierarchy_retry_backoff: float = DEFAULT_HIERARCHY_RETRY_BACKOFF,
        connection_options: Mapping[str, Any] | None = None,
        env: Mapping[str, str] | None = None,
        session_provider: SessionProvider | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._lock = threading.RLock()
        self.block_size = block_size
        self.cache_type = cache_type
        self.num_threads = num_threads
        self.listing_page_size = listing_page_size
        # Redirect is off by default: on iRODS >= 4.3.1 it clones the session to the
        # replica's resource host per open, which races under concurrent opens
        # (HIERARCHY_ERROR) and undermines connection pooling.
        self.allow_redirect = allow_redirect
        self.hierarchy_retries = hierarchy_retries
        self.hierarchy_retry_backoff = hierarchy_retry_backoff

        storage_options = {
            key: value
            for key, value in {
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "zone": zone,
                "irods_env_file": irods_env_file,
                "connection_options": connection_options,
            }.items()
            if value is not None
        }
        self.auth = resolve_auth(
            storage_options, env if env is not None else os.environ
        )
        self._provider = session_provider or SingleSessionProvider(self.auth)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        if isinstance(path, (list, tuple)):
            return [cls._strip_protocol(p) for p in path]  # type: ignore[return-value]
        return normalize_irods_path(path)

    @property
    def session(self) -> iRODSSession:
        """The shared iRODS session, created on first access."""
        return self._provider.session()

    @property
    def lock(self) -> threading.RLock:
        """Guards connection checkout and metadata queries (not byte transfers)."""
        return self._lock

    def _retry_hierarchy(self, operation: Callable[[], Any], path: str) -> Any:
        """Run `operation`, retrying transient iRODS HIERARCHY_ERRORs.

        On iRODS >= 4.3.1 a concurrent open can fail while the server resolves the resource
        hierarchy; a short retry almost always succeeds. Only HIERARCHY_ERROR is retried, and
        only up to `hierarchy_retries` times — every other error propagates immediately.
        """
        attempt = 0
        while True:
            try:
                return operation()
            except HIERARCHY_ERROR:
                if attempt >= self.hierarchy_retries:
                    raise
                attempt += 1
                logger.warning(
                    "iRODS HIERARCHY_ERROR on %s; retry %d/%d. This is the iRODS >= 4.3.1 "
                    "resource-hierarchy race under concurrent opens; a retry usually clears it.",
                    path,
                    attempt,
                    self.hierarchy_retries,
                )
                time.sleep(self.hierarchy_retry_backoff * attempt)

    def _open_data_object(self, path: str, mode: str) -> Any:
        """Open a PRC data-object handle under the lock, retrying HIERARCHY_ERRORs."""

        def opener() -> Any:
            with self._lock:
                return self.session.data_objects.open(
                    path, mode, allow_redirect=self.allow_redirect
                )

        return self._retry_hierarchy(opener, path)

    def close(self) -> None:
        """Release the iRODS session and its connection pool."""
        self._provider.close()

    def invalidate_cache(self, path: str | None = None) -> None:
        if path is None:
            self.dircache.clear()
            return
        norm = self._strip_protocol(path)
        self.dircache.pop(norm, None)
        self.dircache.pop(parent_path(norm), None)

    # --- Read path ------------------------------------------------------------

    def info(self, path: str, **kwargs: Any) -> InfoDict:
        norm = self._strip_protocol(path)
        with self._lock:
            return listing.stat(self.session, norm)

    def ls(self, path: str, detail: bool = True, **kwargs: Any):
        norm = self._strip_protocol(path)
        entries = self.dircache.get(norm)
        if entries is None:
            with self._lock:
                # Re-check under the lock so concurrent ls() of the same path issue one query.
                entries = self.dircache.get(norm)
                if entries is None:
                    details = self.info(norm)
                    if details["type"] == "file":
                        entries = [details]
                    else:
                        entries = listing.list_collection_children(
                            self.session, norm, self.listing_page_size
                        )
                    self.dircache[norm] = entries
        return entries if detail else [entry["name"] for entry in entries]

    def created(self, path: str):
        return self.info(path).get("created")

    def modified(self, path: str):
        return self.info(path).get("modified")

    def walk(
        self,
        path: str,
        maxdepth: int | None = None,
        topdown: bool = True,
        on_error: str = "omit",
        **kwargs: Any,
    ) -> Generator[tuple[str, Any, Any], Any, None]:
        """Recursively walk a subtree using a constant number of GenQueries.

        fsspec's default `walk` issues one `ls` (≈2 catalog queries) per directory; iRODS can
        return a whole subtree with a single `LIKE` filter on the collection path, so this
        fetches the tree once and yields it in the same `(path, dirs, files)` contract.
        `find`/`glob`/`du` are built on `walk`, so they inherit the speedup.
        """
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")
        norm = self._strip_protocol(path)
        detail = kwargs.pop("detail", False)
        try:
            with self._lock:
                if listing.stat(self.session, norm)["type"] != "directory":
                    return  # find()'s isfile fallback handles a file path
                file_infos = listing.walk_data_objects(self.session, norm)
                dir_infos = listing.walk_collections(self.session, norm)
        except (FileNotFoundError, OSError) as exc:
            if on_error == "raise":
                raise
            if callable(on_error):
                on_error(exc)
            return

        children_dirs: dict[str, list[InfoDict]] = {}
        children_files: dict[str, list[InfoDict]] = {}
        for info in file_infos:
            children_files.setdefault(parent_path(info["name"]), []).append(info)
        for info in dir_infos:
            children_dirs.setdefault(parent_path(info["name"]), []).append(info)

        yield from self._walk_subtree(
            norm, maxdepth, topdown, detail, children_dirs, children_files
        )

    def _walk_subtree(
        self,
        node: str,
        maxdepth: int | None,
        topdown: bool,
        detail: bool,
        children_dirs: Mapping[str, list[InfoDict]],
        children_files: Mapping[str, list[InfoDict]],
    ) -> Generator[tuple[str, Any, Any], Any, None]:
        full_dirs = {
            base_name(i["name"]): i["name"] for i in children_dirs.get(node, [])
        }
        dirs = {base_name(i["name"]): i for i in children_dirs.get(node, [])}
        files = {base_name(i["name"]): i for i in children_files.get(node, [])}

        dirs_out: Any = dirs if detail else list(dirs)
        files_out: Any = files if detail else list(files)

        if topdown:
            yield node, dirs_out, files_out

        if maxdepth is not None:
            maxdepth -= 1
            if maxdepth < 1:
                if not topdown:
                    yield node, dirs_out, files_out
                return

        for (
            name
        ) in dirs_out:  # iterating the yielded object honors caller pruning (topdown)
            yield from self._walk_subtree(
                full_dirs[name],
                maxdepth,
                topdown,
                detail,
                children_dirs,
                children_files,
            )

        if not topdown:
            yield node, dirs_out, files_out

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> DucktapeBufferedFile:
        norm = self._strip_protocol(path)
        if "a" in mode:
            raise NotImplementedError("append mode is not supported")
        if any(flag in mode for flag in ("w", "x")):
            self.invalidate_cache(parent_path(norm))
        return DucktapeBufferedFile(
            self,
            norm,
            mode=mode,
            block_size=block_size or self.block_size,
            autocommit=autocommit,
            cache_type=self.cache_type,
            cache_options=cache_options,
            **kwargs,
        )

    def get_file(
        self,
        rpath: str,
        lpath: str,
        callback: Callback = DEFAULT_CALLBACK,
        outfile: Any = None,
        **kwargs: Any,
    ) -> None:
        """Download a whole data object to a local path using PRC parallel transfer.

        This bypasses the streaming buffered-file path: for whole-file local copies of
        large objects, PRC's multi-threaded `get` (auto-parallel above 32 MB on iRODS
        4.2.9+) is far faster than streaming byte ranges through Python.
        """
        norm = self._strip_protocol(rpath)
        info = self.info(norm)
        if info["type"] == "directory":
            os.makedirs(lpath, exist_ok=True)
            return
        callback.set_size(info["size"])
        with self._lock:
            session = self.session
        with _translate_errors(norm):
            self._retry_hierarchy(
                lambda: session.data_objects.get(
                    norm,
                    lpath,
                    num_threads=self.num_threads,
                    updatables=(_threadsafe_progress(callback),),
                ),
                norm,
            )

    # --- Write path -----------------------------------------------------------

    def mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        norm = self._strip_protocol(path)
        with self._lock, _translate_errors(norm):
            self.session.collections.create(norm, recurse=create_parents)
        self.invalidate_cache(norm)

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        norm = self._strip_protocol(path)
        if not exist_ok and self.exists(norm):
            raise FileExistsError(norm)
        with self._lock, _translate_errors(norm):
            self.session.collections.create(norm, recurse=True)
        self.invalidate_cache(norm)

    def rmdir(self, path: str) -> None:
        norm = self._strip_protocol(path)
        with self._lock, _translate_errors(norm):
            self.session.collections.remove(norm, recurse=False)
        self.invalidate_cache(norm)

    def rm_file(self, path: str) -> None:
        """Delete a data object. Idempotent: a missing object is a no-op.

        Existence is checked first rather than relying on the error raised for a missing
        object: iRODS rule engines (e.g. CyVerse) may surface that as a generic policy
        error like CUT_ACTION_PROCESSED_ERR rather than DataObjectDoesNotExist.
        """
        norm = self._strip_protocol(path)
        if not self.exists(norm):
            return
        try:
            with self._lock:
                self.session.data_objects.unlink(norm, force=True)
        except (DataObjectDoesNotExist, CAT_NO_ROWS_FOUND):
            pass  # lost a race with another deleter
        self.invalidate_cache(norm)

    def cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        src = self._strip_protocol(path1)
        dst = self._strip_protocol(path2)
        with self._lock, _translate_errors(src):
            self.session.data_objects.copy(src, dst)
        self.invalidate_cache(dst)

    def put_file(
        self,
        lpath: str,
        rpath: str,
        callback: Callback = DEFAULT_CALLBACK,
        mode: str = "overwrite",
        **kwargs: Any,
    ) -> None:
        """Upload a whole local file using PRC parallel transfer (see `get_file`)."""
        norm = self._strip_protocol(rpath)
        if os.path.isdir(lpath):
            self.makedirs(norm, exist_ok=True)
            return
        callback.set_size(os.path.getsize(lpath))
        with self._lock:
            session = self.session
        with _translate_errors(norm):
            self._retry_hierarchy(
                lambda: session.data_objects.put(
                    lpath,
                    norm,
                    num_threads=self.num_threads,
                    updatables=(_threadsafe_progress(callback),),
                ),
                norm,
            )
        self.invalidate_cache(norm)
