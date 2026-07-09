"""`DucktapeBufferedFile`: fsspec buffered file backed by an iRODS data object handle.

The read path holds a single PRC data-object handle open for the lifetime of the file
and serves `_fetch_range` by seeking on it, so DuckDB's many small Parquet range reads do
not each pay an open/auth round trip. The handle is opened lazily on first read (under the
filesystem lock, since opening checks a connection out of the pool); subsequent seeks and
reads run without the lock because the handle owns its own connection.

The write path opens the handle in `_initiate_upload` and streams each buffered block to
it in `_upload_chunk`, so a multi-gigabyte write never stages the whole object in memory.

Transactional writes (`autocommit=False`, e.g. inside `with fs.transaction:`) stream to a
hidden staging object in the same collection; `commit()` renames it over the final path (a
cheap catalog operation) and `discard()` unlinks it. A crash between close and commit can
leave a `.<name>.ducktape-tmp-*` orphan behind; they are never swept automatically.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from fsspec.spec import AbstractBufferedFile

from .paths import ROOT, base_name, parent_path

if TYPE_CHECKING:
    from .filesystem import DucktapeFileSystem

logger = logging.getLogger("ducktape")


def _staging_path(path: str) -> str:
    """A hidden, collision-safe sibling path used to stage a transactional write."""
    parent = parent_path(path)
    prefix = "" if parent == ROOT else parent
    return f"{prefix}/.{base_name(path)}.ducktape-tmp-{uuid4().hex[:8]}"


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
        # Where a transactional (autocommit=False) write is staged until commit().
        self._staging_path: str | None = None
        # Guards the single shared PRC handle: DuckDB issues parallel range reads against the
        # same file object, and seek()+read() on one handle is not atomic. This serializes
        # range reads within a single file (handles for different files stay independent).
        self._handle_lock = threading.Lock()
        # A collection stats fine (type "directory", size 0), so without this check
        # reading one would silently return b"" instead of failing. Only when size is
        # unset: then details were already fetched (no extra round trip), and an explicit
        # size is the caller asserting the path is a file. Runs after the handle
        # attributes exist so __del__ -> close() on the failed object stays safe.
        if self.mode == "rb" and size is None and self.details["type"] == "directory":
            raise IsADirectoryError(path)

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
        target = self.path
        if not self.autocommit:
            # Stage in the same collection so commit() is a cheap catalog rename.
            target = _staging_path(self.path)
            self._staging_path = target
        self._handle = self.fs._open_data_object(target, "w")

    def _upload_chunk(self, final: bool = False) -> None:
        if self._handle is None:
            self._initiate_upload()
        handle = self._handle
        assert handle is not None
        handle.write(self.buffer.getvalue())

    def commit(self) -> None:
        """Publish a transactional write by renaming the staged object over the target."""
        if self._staging_path is None:
            return
        self.fs._move_file(self._staging_path, self.path)
        self._staging_path = None

    def discard(self) -> None:
        """Roll back a transactional write by unlinking the staged object."""
        if self._staging_path is None:
            return
        self.fs.rm_file(self._staging_path)
        self._staging_path = None

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
