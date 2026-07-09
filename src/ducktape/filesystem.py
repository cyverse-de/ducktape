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
from typing import TYPE_CHECKING, Any, cast

from fsspec.callbacks import DEFAULT_CALLBACK, Callback
from fsspec.spec import AbstractFileSystem
from irods import keywords as kw
from irods.exception import (
    CAT_COLLECTION_NOT_EMPTY,
    CAT_NO_ROWS_FOUND,
    HIERARCHY_ERROR,
    DoesNotExist,
    PycommandsException,
    iRODSException,
)

from . import listing
from .auth import DEFAULT_PORT, SessionProvider, SingleSessionProvider, resolve_auth
from .errors import IrodsFileNotFoundError, IrodsNotEmptyError, IrodsOperationError
from .file import DucktapeBufferedFile
from .listing import InfoDict
from .paths import base_name, is_under, normalize_irods_path, parent_path

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

    def _retry_hierarchy(
        self,
        operation: Callable[[], Any],
        path: str,
        on_retry: Callable[[], None] | None = None,
    ) -> Any:
        """Run `operation`, retrying transient iRODS HIERARCHY_ERRORs.

        On iRODS >= 4.3.1 a concurrent open can fail while the server resolves the resource
        hierarchy; a short retry almost always succeeds. Only HIERARCHY_ERROR is retried, and
        only up to `hierarchy_retries` times — every other error propagates immediately.
        `on_retry` runs before each retry (e.g. to reset a progress callback that the failed
        attempt may have partially advanced).
        """
        attempt = 0
        while True:
            try:
                return operation()
            except HIERARCHY_ERROR:
                if attempt >= self.hierarchy_retries:
                    raise
                attempt += 1
                if on_retry is not None:
                    on_retry()
                logger.warning(
                    "iRODS HIERARCHY_ERROR on %s; retry %d/%d. This is the iRODS >= 4.3.1 "
                    "resource-hierarchy race under concurrent opens; a retry usually clears it.",
                    path,
                    attempt,
                    self.hierarchy_retries,
                )
                time.sleep(self.hierarchy_retry_backoff * attempt)

    def _open_data_object(self, path: str, mode: str) -> Any:
        """Open a PRC data-object handle under the lock, retrying HIERARCHY_ERRORs.

        Errors are translated after the retry layer so the retry still sees the raw
        HIERARCHY_ERROR; only an exhausted retry surfaces it, as an `IrodsOperationError`.
        """

        def opener() -> Any:
            with self._lock:
                return self.session.data_objects.open(
                    path, mode, allow_redirect=self.allow_redirect
                )

        with _translate_errors(path):
            return self._retry_hierarchy(opener, path)

    def close(self) -> None:
        """Release the iRODS session and its connection pool."""
        self._provider.close()

    def __enter__(self) -> DucktapeFileSystem:
        return self

    def __exit__(self, *exc_info: object) -> None:
        # Best used with skip_instance_cache=True: fsspec caches instances by default,
        # and closing a cached instance closes it for every other holder too.
        self.close()

    def invalidate_cache(self, path: str | None = None) -> None:
        if path is None:
            self.dircache.clear()
            return
        norm = self._strip_protocol(path)
        self.dircache.pop(norm, None)
        self.dircache.pop(parent_path(norm), None)

    def _invalidate_subtree(self, root: str) -> None:
        """Evict `root`, its parent, and every cached descendant listing.

        Needed when a whole collection changes path (mv): descendants cached by `walk`'s
        dircache seeding would otherwise keep serving their old paths. Iterating the whole
        dircache needs the lock — a concurrent ls() inserting a key mid-scan would raise.
        """
        with self._lock:
            for key in [key for key in self.dircache if is_under(key, root)]:
                self.dircache.pop(key, None)
            self.dircache.pop(parent_path(root), None)

    # --- Read path ------------------------------------------------------------

    def info(self, path: str, **kwargs: Any) -> InfoDict:
        norm = self._strip_protocol(path)
        with self._lock, _translate_errors(norm):
            return listing.stat(self.session, norm)

    def exists(self, path: str, **kwargs: Any) -> bool:
        """True if the path exists; only "not found" means False.

        Overrides fsspec's default, whose bare `except` treats *any* failure (network
        drop, auth expiry) as "does not exist" — which would make the idempotent deletes
        below silently no-op instead of surfacing the real error.
        """
        try:
            self.info(path, **kwargs)
            return True
        except FileNotFoundError:
            return False

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
                        with _translate_errors(norm):
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
    ) -> Generator[Any, Any, None]:
        """Recursively walk a subtree.

        For an unbounded walk, iRODS can return the whole subtree with a single `LIKE`
        filter on the collection path, so this fetches the tree once and yields it in the
        standard `(path, dirs, files)` contract — `find`/`glob` (unbounded) inherit that
        speedup. A bounded walk (`maxdepth` set) delegates to fsspec's per-directory `ls`,
        which fetches only the requested depth instead of the entire subtree.
        """
        if maxdepth is not None and maxdepth < 1:
            raise ValueError("maxdepth must be at least 1")
        if maxdepth is not None:
            yield from super().walk(path, maxdepth, topdown, on_error, **kwargs)
            return

        norm = self._strip_protocol(path)
        detail = kwargs.pop("detail", False)
        try:
            # Acquire the lock separately per query: each GenQuery's continuation must page
            # on a consistent pool connection (so it can't be interleaved), but releasing
            # between queries lets concurrent opens/info proceed instead of waiting out the
            # whole subtree scan.
            with self._lock, _translate_errors(norm):
                is_directory = listing.stat(self.session, norm)["type"] == "directory"
            if not is_directory:
                return  # find()'s isfile fallback handles a file path
            with self._lock, _translate_errors(norm):
                file_infos = listing.walk_data_objects(self.session, norm)
            with self._lock, _translate_errors(norm):
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

        # Seed dircache so a later ls() of any walked directory is served from cache.
        for node in (norm, *(info["name"] for info in dir_infos)):
            self.dircache[node] = children_files.get(node, []) + children_dirs.get(
                node, []
            )

        yield from self._walk_subtree(
            norm, topdown, detail, children_dirs, children_files
        )

    def _walk_subtree(
        self,
        node: str,
        topdown: bool,
        detail: bool,
        children_dirs: Mapping[str, list[InfoDict]],
        children_files: Mapping[str, list[InfoDict]],
    ) -> Generator[tuple[str, Any, Any], Any, None]:
        dirs = {base_name(i["name"]): i for i in children_dirs.get(node, [])}
        files = {base_name(i["name"]): i for i in children_files.get(node, [])}

        dirs_out: Any = dirs if detail else list(dirs)
        files_out: Any = files if detail else list(files)

        if topdown:
            yield node, dirs_out, files_out
        # Iterating dirs_out (the yielded object) honors caller pruning when topdown.
        for name in dirs_out:
            yield from self._walk_subtree(
                dirs[name]["name"], topdown, detail, children_dirs, children_files
            )
        if not topdown:
            yield node, dirs_out, files_out

    def du(
        self,
        path: str,
        total: bool = True,
        maxdepth: int | None = None,
        withdirs: bool = False,
        **kwargs: Any,
    ) -> Any:
        """Disk usage from one subtree walk, using the sizes `find` already returns.

        Overridden so it does not fall back to fsspec's default (a `self.info` per file).
        """
        infos = cast(
            "dict[str, InfoDict]",
            self.find(path, maxdepth=maxdepth, withdirs=withdirs, detail=True),
        )
        sizes = {p: int(info.get("size") or 0) for p, info in infos.items()}
        if total:
            return sum(sizes.values())
        return sizes

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
        # Client-side check only (TOCTOU window): PRC's open cannot express O_EXCL.
        if "x" in mode and self.exists(norm):
            raise FileExistsError(norm)
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
                on_retry=lambda: callback.absolute_update(0),
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
        """Remove an empty collection. Idempotent: a missing collection is a no-op."""
        norm = self._strip_protocol(path)
        if not self.exists(norm):
            return
        try:
            with self._lock, _translate_errors(norm):
                self.session.collections.remove(norm, recurse=False)
        except IrodsFileNotFoundError:
            pass  # lost a race with another remover
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
            with self._lock, _translate_errors(norm):
                self.session.data_objects.unlink(norm, force=True)
        except IrodsFileNotFoundError:
            pass  # lost a race with another deleter
        self.invalidate_cache(norm)

    def cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        src = self._strip_protocol(path1)
        dst = self._strip_protocol(path2)
        # Force overwrite: iRODS rejects a copy onto an existing data object without the force
        # flag, but fsspec's mv (copy + rm) is how DuckDB renames its temp output over the
        # final target, so a second COPY TO the same path must clobber the prior object.
        # Label errors with both paths: a copy can fail because of either the source or the
        # destination, so naming only one would point debugging at the wrong path.
        with self._lock, _translate_errors(f"{src} -> {dst}"):
            self.session.data_objects.copy(src, dst, **{kw.FORCE_FLAG_KW: ""})
        self.invalidate_cache(dst)

    def mv(
        self,
        path1: str,
        path2: str,
        recursive: bool = False,
        maxdepth: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Rename via the iRODS catalog instead of fsspec's server-side copy + delete.

        A catalog rename is a metadata operation regardless of object size, which is what
        DuckDB's temp->target rename of a multi-gigabyte COPY TO output needs. Falls back
        to fsspec's copy+rm for the cases a rename cannot express: bounded-depth moves,
        and a collection moved onto an existing destination (iRODS would move *into* it,
        which differs from fsspec's merge semantics).
        """
        src = self._strip_protocol(path1)
        dst = self._strip_protocol(path2)
        if src == dst:
            return
        if maxdepth is not None:
            super().mv(path1, path2, recursive=recursive, maxdepth=maxdepth, **kwargs)
            return
        try:
            dst_type: str | None = self.info(dst)["type"]
        except FileNotFoundError:
            dst_type = None
        if self.info(src)["type"] == "directory":
            if not recursive or dst_type is not None:
                super().mv(path1, path2, recursive=recursive, **kwargs)
                return
            with self._lock, _translate_errors(f"{src} -> {dst}"):
                self.session.collections.move(src, dst)
            self._invalidate_subtree(src)
            self.invalidate_cache(dst)
        elif dst_type == "directory":
            # Moving a file onto an existing collection: defer to fsspec's semantics
            # rather than guessing between replace and move-into.
            super().mv(path1, path2, recursive=recursive, **kwargs)
        else:
            self._move_file(src, dst)

    def _move_file(self, src: str, dst: str) -> None:
        """Rename one data object, clobbering an existing destination (see `cp_file`).

        iRODS refuses to rename onto an existing data object, so the destination is
        unlinked first. Not atomic: a crash between the unlink and the move loses the old
        destination, but never the source data.
        """
        self.rm_file(dst)
        with self._lock, _translate_errors(f"{src} -> {dst}"):
            self.session.data_objects.move(src, dst)
        self.invalidate_cache(src)
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
        if mode == "create" and self.exists(norm):
            raise FileExistsError(norm)
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
                on_retry=lambda: callback.absolute_update(0),
            )
        self.invalidate_cache(norm)
