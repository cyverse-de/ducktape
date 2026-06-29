from __future__ import annotations

from typing import Any

import pytest
from irods.exception import CAT_COLLECTION_NOT_EMPTY, DataObjectDoesNotExist

from ducktape import listing
from ducktape.errors import IrodsFileNotFoundError, IrodsNotEmptyError
from ducktape.filesystem import DucktapeFileSystem


class WriteHandle:
    def __init__(self) -> None:
        self.written = b""
        self.closed = False

    def write(self, data: bytes) -> int:
        self.written += data
        return len(data)

    def close(self) -> None:
        self.closed = True


class RecordingDataObjects:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.handle = WriteHandle()
        self.unlink_raises: Exception | None = None

    def open(self, path: str, mode: str, allow_redirect: object = None) -> WriteHandle:
        self.calls.append(("open", path, mode, allow_redirect))
        return self.handle

    def unlink(self, path: str, force: bool = False) -> None:
        self.calls.append(("unlink", path, force))
        if self.unlink_raises is not None:
            raise self.unlink_raises

    def copy(self, src: str, dst: str) -> None:
        self.calls.append(("copy", src, dst))

    def put(self, lpath: str, rpath: str, num_threads: int = 0) -> None:
        self.calls.append(("put", lpath, rpath, num_threads))


class RecordingCollections:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.remove_raises: Exception | None = None

    def create(self, path: str, recurse: bool = True) -> None:
        self.calls.append(("create", path, recurse))

    def remove(self, path: str, recurse: bool = True, force: bool = False) -> None:
        self.calls.append(("remove", path, recurse, force))
        if self.remove_raises is not None:
            raise self.remove_raises


class RecordingSession:
    def __init__(self) -> None:
        self.data_objects = RecordingDataObjects()
        self.collections = RecordingCollections()


class StubProvider:
    def __init__(self, session: Any) -> None:
        self._session = session

    def session(self) -> Any:
        return self._session

    def close(self) -> None:
        pass


def make_fs(session: Any) -> DucktapeFileSystem:
    return DucktapeFileSystem(
        host="irods.example.org",
        user="rods",
        password="secret",
        zone="tempZone",
        skip_instance_cache=True,
        session_provider=StubProvider(session),
    )


def test_write_streams_through_handle() -> None:
    session = RecordingSession()
    fs = make_fs(session)
    with fs.open("/z/home/rods/out.bin", "wb") as raw:
        handle: Any = raw
        handle.write(b"hello world")
    do = session.data_objects
    assert do.handle.written == b"hello world"
    assert do.handle.closed is True
    assert ("open", "/z/home/rods/out.bin", "w", False) in do.calls


def test_mkdir_creates_collection() -> None:
    session = RecordingSession()
    fs = make_fs(session)
    fs.mkdir("/z/home/rods/sub")
    assert ("create", "/z/home/rods/sub", True) in session.collections.calls


def test_rmdir_maps_not_empty() -> None:
    session = RecordingSession()
    session.collections.remove_raises = CAT_COLLECTION_NOT_EMPTY("not empty")
    fs = make_fs(session)
    with pytest.raises(IrodsNotEmptyError):
        fs.rmdir("/z/home/rods/sub")


def test_rm_file_deletes_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    session = RecordingSession()
    fs = make_fs(session)
    monkeypatch.setattr(
        listing, "stat", lambda s, p: {"name": p, "type": "file", "size": 1}
    )
    fs.rm_file("/z/home/rods/a.bin")
    assert ("unlink", "/z/home/rods/a.bin", True) in session.data_objects.calls


def test_rm_file_missing_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    session = RecordingSession()
    fs = make_fs(session)

    def raise_missing(s: object, p: str) -> dict:
        raise IrodsFileNotFoundError(p)

    monkeypatch.setattr(listing, "stat", raise_missing)
    fs.rm_file("/z/home/rods/missing.bin")  # must not raise
    assert all(call[0] != "unlink" for call in session.data_objects.calls)


def test_rm_file_swallows_delete_race(monkeypatch: pytest.MonkeyPatch) -> None:
    session = RecordingSession()
    session.data_objects.unlink_raises = DataObjectDoesNotExist("gone")
    fs = make_fs(session)
    monkeypatch.setattr(
        listing, "stat", lambda s, p: {"name": p, "type": "file", "size": 1}
    )
    fs.rm_file("/z/home/rods/raced.bin")  # must not raise


def test_cp_file_server_side_copy() -> None:
    session = RecordingSession()
    fs = make_fs(session)
    fs.cp_file("/z/home/rods/a.bin", "/z/home/rods/b.bin")
    assert (
        "copy",
        "/z/home/rods/a.bin",
        "/z/home/rods/b.bin",
    ) in session.data_objects.calls


def test_put_file_uses_parallel_transfer() -> None:
    session = RecordingSession()
    fs = make_fs(session)
    fs.put_file("/tmp/local.bin", "/z/home/rods/remote.bin")
    assert (
        "put",
        "/tmp/local.bin",
        "/z/home/rods/remote.bin",
        0,
    ) in session.data_objects.calls


def test_makedirs_existing_without_exist_ok_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = RecordingSession()
    fs = make_fs(session)
    monkeypatch.setattr(
        listing, "stat", lambda s, p: {"name": p, "type": "directory", "size": 0}
    )
    with pytest.raises(FileExistsError):
        fs.makedirs("/z/home/rods/exists", exist_ok=False)


def test_append_mode_unsupported() -> None:
    session = RecordingSession()
    fs = make_fs(session)
    with pytest.raises(NotImplementedError):
        fs.open("/z/home/rods/x.bin", "ab")
