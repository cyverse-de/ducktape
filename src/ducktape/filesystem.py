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
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from fsspec.callbacks import DEFAULT_CALLBACK, Callback
from fsspec.spec import AbstractFileSystem
from irods.exception import (
    CAT_COLLECTION_NOT_EMPTY,
    CAT_NO_ROWS_FOUND,
    DataObjectDoesNotExist,
)

from . import listing
from .auth import DEFAULT_PORT, SessionProvider, SingleSessionProvider, resolve_auth
from .errors import IrodsNotEmptyError
from .file import DucktapeBufferedFile
from .listing import InfoDict
from .paths import normalize_irods_path, parent_path

if TYPE_CHECKING:
    from irods.session import iRODSSession

logger = logging.getLogger("ducktape")

DEFAULT_BLOCK_SIZE = 4 * 2**20
DEFAULT_CACHE_TYPE = "readahead"


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

        connection_options = {
            key: value
            for key, value in {
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "zone": zone,
                "irods_env_file": irods_env_file,
            }.items()
            if value is not None
        }
        self.auth = resolve_auth(
            connection_options, env if env is not None else os.environ
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
            details = self.info(norm)
            if details["type"] == "file":
                entries = [details]
            else:
                with self._lock:
                    entries = listing.list_collection_children(
                        self.session, norm, self.listing_page_size
                    )
            self.dircache[norm] = entries
        return entries if detail else [entry["name"] for entry in entries]

    def created(self, path: str):
        return self.info(path).get("created")

    def modified(self, path: str):
        return self.info(path).get("modified")

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
        if self.isdir(norm):
            os.makedirs(lpath, exist_ok=True)
            return
        with self._lock:
            session = self.session
        session.data_objects.get(norm, lpath, num_threads=self.num_threads)

    # --- Write path -----------------------------------------------------------

    def mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        norm = self._strip_protocol(path)
        with self._lock:
            self.session.collections.create(norm, recurse=create_parents)
        self.invalidate_cache(norm)

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        norm = self._strip_protocol(path)
        if not exist_ok and self.exists(norm):
            raise FileExistsError(norm)
        with self._lock:
            self.session.collections.create(norm, recurse=True)
        self.invalidate_cache(norm)

    def rmdir(self, path: str) -> None:
        norm = self._strip_protocol(path)
        try:
            with self._lock:
                self.session.collections.remove(norm, recurse=False)
        except CAT_COLLECTION_NOT_EMPTY as exc:
            raise IrodsNotEmptyError(norm) from exc
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
        with self._lock:
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
        with self._lock:
            session = self.session
        session.data_objects.put(lpath, norm, num_threads=self.num_threads)
        self.invalidate_cache(norm)
