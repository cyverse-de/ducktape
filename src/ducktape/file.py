"""`DucktapeBufferedFile`: fsspec buffered file backed by an iRODS data object handle.

The read path holds a single PRC data-object handle open for the lifetime of the file
and serves `_fetch_range` by seeking on it, so DuckDB's many small Parquet range reads do
not each pay an open/auth round trip. The handle is opened lazily on first read (under the
filesystem lock, since opening checks a connection out of the pool); subsequent seeks and
reads run without the lock because the handle owns its own connection.

The write path opens the handle in `_initiate_upload` and streams each buffered block to
it in `_upload_chunk`, so a multi-gigabyte write never stages the whole object in memory.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any

from fsspec.spec import AbstractBufferedFile

if TYPE_CHECKING:
    from .filesystem import DucktapeFileSystem

logger = logging.getLogger("ducktape")


class DucktapeBufferedFile(AbstractBufferedFile):
    """Buffered read access to an iRODS data object."""

    fs: DucktapeFileSystem

    def __init__(
        self,
        fs: DucktapeFileSystem,
        path: str,
        mode: str = "rb",
        block_size: Any = "default",
        autocommit: bool = True,
        cache_type: str = "readahead",
        cache_options: dict[str, Any] | None = None,
        size: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            fs,
            path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            size=size,
            **kwargs,
        )
        self._handle: Any | None = None
        # Guards the single shared PRC handle: DuckDB issues parallel range reads against the
        # same file object, and seek()+read() on one handle is not atomic. This serializes
        # range reads within a single file (handles for different files stay independent).
        self._handle_lock = threading.Lock()

    def _ensure_handle(self) -> Any:
        if self._handle is None:
            self._handle = self.fs._open_data_object(self.path, "r")
        return self._handle

    def _fetch_range(self, start: int, end: int) -> bytes:
        with self._handle_lock:
            handle = self._ensure_handle()
            handle.seek(start)
            return handle.read(end - start)

    def _initiate_upload(self) -> None:
        self._handle = self.fs._open_data_object(self.path, "w")

    def _upload_chunk(self, final: bool = False) -> None:
        if self._handle is None:
            self._initiate_upload()
        handle = self._handle
        assert handle is not None
        handle.write(self.buffer.getvalue())

    def close(self) -> None:
        try:
            super().close()
        finally:
            # Hold the per-file lock so we never close the handle out from under an
            # in-flight _fetch_range (which seeks+reads on this same handle).
            with self._handle_lock:
                handle, self._handle = self._handle, None
                if handle is not None:
                    try:
                        handle.close()
                    except Exception:
                        # The connection may already be gone (e.g. closed during shutdown
                        # via __del__); releasing it best-effort avoids noisy errors.
                        logger.debug(
                            "error closing iRODS handle for %s",
                            self.path,
                            exc_info=True,
                        )
