from __future__ import annotations

from typing import Any, cast

import pytest
from fsspec.callbacks import Callback
from irods import keywords as kw
from irods.exception import (
    CAT_COLLECTION_NOT_EMPTY,
    CAT_NO_ROWS_FOUND,
    HIERARCHY_ERROR,
    CollectionDoesNotExist,
    DataObjectDoesNotExist,
    NetworkException,
)

from ducktape import listing
from ducktape.errors import (
    IrodsFileNotFoundError,
    IrodsNotEmptyError,
    IrodsOperationError,
)
from ducktape.file import DucktapeBufferedFile
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
        self.copy_raises: Exception | None = None
        self.move_raises: Exception | None = None

    def open(self, path: str, mode: str, allow_redirect: object = None) -> WriteHandle:
        self.calls.append(("open", path, mode, allow_redirect))
        return self.handle

    def unlink(self, path: str, force: bool = False) -> None:
        self.calls.append(("unlink", path, force))
        if self.unlink_raises is not None:
            raise self.unlink_raises

    def copy(self, src: str, dst: str, **options: object) -> None:
        self.calls.append(("copy", src, dst, options))
        if self.copy_raises is not None:
            raise self.copy_raises

    def move(self, src: str, dst: str) -> None:
        self.calls.append(("move", src, dst))
        if self.move_raises is not None:
            raise self.move_raises

    def put(
        self,
        lpath: str,
        rpath: str,
        num_threads: int = 0,
        updatables: tuple = (),
    ) -> None:
        self.calls.append(("put", lpath, rpath, num_threads))
        for update in updatables:
            update(7)  # simulate one chunk so progress wiring is exercised


class RecordingCollections:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.remove_raises: Exception | None = None
        self.create_raises: Exception | None = None

    def create(self, path: str, recurse: bool = True) -> None:
        self.calls.append(("create", path, recurse))
        if self.create_raises is not None:
            raise self.create_raises

    def remove(self, path: str, recurse: bool = True, force: bool = False) -> None:
        self.calls.append(("remove", path, recurse, force))
        if self.remove_raises is not None:
            raise self.remove_raises

    def move(self, src: str, dst: str) -> None:
        self.calls.append(("move", src, dst))


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


def _existing_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        listing, "stat", lambda s, p: {"name": p, "type": "directory", "size": 0}
    )


def test_rmdir_maps_not_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    _existing_directory(monkeypatch)
    session = RecordingSession()
    session.collections.remove_raises = CAT_COLLECTION_NOT_EMPTY("not empty")
    fs = make_fs(session)
    with pytest.raises(IrodsNotEmptyError):
        fs.rmdir("/z/home/rods/sub")


def test_rmdir_missing_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    session = RecordingSession()
    fs = make_fs(session)

    def raise_missing(s: object, p: str) -> dict:
        raise IrodsFileNotFoundError(p)

    monkeypatch.setattr(listing, "stat", raise_missing)
    fs.rmdir("/z/home/rods/missing")  # idempotent: must not raise
    assert all(call[0] != "remove" for call in session.collections.calls)


def test_rmdir_swallows_delete_race(monkeypatch: pytest.MonkeyPatch) -> None:
    _existing_directory(monkeypatch)
    session = RecordingSession()
    session.collections.remove_raises = CollectionDoesNotExist("gone")
    fs = make_fs(session)
    fs.rmdir("/z/home/rods/raced")  # exists() passed, then removed by another: no raise


def test_rmdir_maps_operation_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _existing_directory(monkeypatch)
    session = RecordingSession()
    session.collections.remove_raises = NetworkException("boom")
    fs = make_fs(session)
    with pytest.raises(IrodsOperationError):
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
        {kw.FORCE_FLAG_KW: ""},
    ) in session.data_objects.calls


def test_put_file_uses_parallel_transfer(tmp_path: Any) -> None:
    session = RecordingSession()
    fs = make_fs(session)
    local = tmp_path / "local.bin"
    local.write_bytes(b"payload!")
    fs.put_file(str(local), "/z/home/rods/remote.bin")
    assert (
        "put",
        str(local),
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


_ERROR_MAP = [
    (CollectionDoesNotExist, IrodsFileNotFoundError),
    (CAT_NO_ROWS_FOUND, IrodsFileNotFoundError),
    (NetworkException, IrodsOperationError),
]


@pytest.mark.parametrize("operation", ["mkdir", "cp_file"])
@pytest.mark.parametrize(("prc_error", "expected"), _ERROR_MAP)
def test_write_ops_map_prc_errors(
    operation: str,
    prc_error: type[Exception],
    expected: type[Exception],
) -> None:
    session = RecordingSession()
    if operation == "mkdir":
        session.collections.create_raises = prc_error("boom")
        action = lambda: make_fs(session).mkdir("/z/d")  # noqa: E731
    else:
        session.data_objects.copy_raises = prc_error("boom")
        action = lambda: make_fs(session).cp_file("/z/a", "/z/b")  # noqa: E731
    with pytest.raises(expected):
        action()


def test_get_file_progress_drives_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    session = RecordingSession()

    def fake_get(
        path: str, lpath: str, num_threads: int = 0, updatables: tuple = ()
    ) -> None:
        for update in updatables:
            update(40)
            update(60)

    session.data_objects.get = fake_get  # type: ignore[attr-defined]
    fs = make_fs(session)
    monkeypatch.setattr(
        listing, "stat", lambda s, p: {"name": p, "type": "file", "size": 100}
    )
    callback = Callback()
    fs.get_file("/z/home/rods/a.bin", "/tmp/out.bin", callback=callback)
    assert callback.size == 100
    assert callback.value == 100


def test_put_file_progress_drives_callback(tmp_path: Any) -> None:
    session = RecordingSession()

    def fake_put(
        lpath: str, rpath: str, num_threads: int = 0, updatables: tuple = ()
    ) -> None:
        for update in updatables:
            update(8)

    session.data_objects.put = fake_put  # type: ignore[attr-defined]
    fs = make_fs(session)
    local = tmp_path / "in.bin"
    local.write_bytes(b"12345678")
    callback = Callback()
    fs.put_file(str(local), "/z/home/rods/remote.bin", callback=callback)
    assert callback.size == 8
    assert callback.value == 8


def test_get_file_progress_resets_on_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    session = RecordingSession()
    attempts = {"n": 0}

    def fake_get(
        path: str, lpath: str, num_threads: int = 0, updatables: tuple = ()
    ) -> None:
        attempts["n"] += 1
        if attempts["n"] == 1:
            for update in updatables:
                update(40)  # partial progress before the transient failure
            raise HIERARCHY_ERROR("resource hierarchy race")
        for update in updatables:
            update(100)

    session.data_objects.get = fake_get  # type: ignore[attr-defined]
    fs = DucktapeFileSystem(
        host="irods.example.org",
        user="rods",
        password="secret",
        zone="tempZone",
        hierarchy_retry_backoff=0,
        skip_instance_cache=True,
        session_provider=StubProvider(session),
    )
    monkeypatch.setattr(
        listing, "stat", lambda s, p: {"name": p, "type": "file", "size": 100}
    )
    callback = Callback()
    fs.get_file("/z/home/rods/a.bin", "/tmp/out.bin", callback=callback)
    assert attempts["n"] == 2
    assert callback.value == 100  # reset on retry, not 140


def _stat_map(entries: dict[str, dict]) -> Any:
    """A listing.stat stub backed by a fixed path->info map; everything else is missing."""

    def stat(session: object, path: str) -> dict:
        try:
            return entries[path]
        except KeyError:
            raise IrodsFileNotFoundError(path) from None

    return stat


def _file(path: str) -> dict:
    return {"name": path, "type": "file", "size": 1}


def _dir(path: str) -> dict:
    return {"name": path, "type": "directory", "size": 0}


def test_rm_file_maps_operation_error(monkeypatch: pytest.MonkeyPatch) -> None:
    session = RecordingSession()
    session.data_objects.unlink_raises = NetworkException("boom")
    fs = make_fs(session)
    monkeypatch.setattr(listing, "stat", _stat_map({"/z/a.bin": _file("/z/a.bin")}))
    with pytest.raises(IrodsOperationError):
        fs.rm_file("/z/a.bin")


def test_rm_file_probe_failure_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    """A network error during the existence probe must raise, not silently skip."""
    session = RecordingSession()
    fs = make_fs(session)

    def raise_network(session: object, path: str) -> dict:
        raise NetworkException("connection lost")

    monkeypatch.setattr(listing, "stat", raise_network)
    with pytest.raises(IrodsOperationError):
        fs.rm_file("/z/a.bin")
    assert all(call[0] != "unlink" for call in session.data_objects.calls)


def test_put_file_create_mode_rejects_existing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    session = RecordingSession()
    fs = make_fs(session)
    monkeypatch.setattr(
        listing, "stat", _stat_map({"/z/exists.bin": _file("/z/exists.bin")})
    )
    local = tmp_path / "in.bin"
    local.write_bytes(b"x")
    with pytest.raises(FileExistsError):
        fs.put_file(str(local), "/z/exists.bin", mode="create")
    assert all(call[0] != "put" for call in session.data_objects.calls)


def test_exclusive_create_rejects_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    session = RecordingSession()
    fs = make_fs(session)
    monkeypatch.setattr(
        listing, "stat", _stat_map({"/z/exists.bin": _file("/z/exists.bin")})
    )
    with pytest.raises(FileExistsError):
        fs.open("/z/exists.bin", "xb")


def test_exclusive_create_writes_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    session = RecordingSession()
    fs = make_fs(session)
    monkeypatch.setattr(listing, "stat", _stat_map({}))
    with fs.open("/z/new.bin", "xb") as raw:
        handle: Any = raw
        handle.write(b"fresh")
    do = session.data_objects
    assert ("open", "/z/new.bin", "w", False) in do.calls
    assert do.handle.written == b"fresh"


def test_mv_file_renames_without_unlink_when_dst_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = RecordingSession()
    fs = make_fs(session)
    monkeypatch.setattr(listing, "stat", _stat_map({"/z/src.bin": _file("/z/src.bin")}))
    fs.mv("/z/src.bin", "/z/dst.bin")
    do = session.data_objects
    assert ("move", "/z/src.bin", "/z/dst.bin") in do.calls
    assert all(call[0] != "unlink" for call in do.calls)


def test_mv_file_unlinks_existing_dst_before_rename(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = RecordingSession()
    fs = make_fs(session)
    monkeypatch.setattr(
        listing,
        "stat",
        _stat_map(
            {"/z/src.bin": _file("/z/src.bin"), "/z/dst.bin": _file("/z/dst.bin")}
        ),
    )
    fs.mv("/z/src.bin", "/z/dst.bin")
    do = session.data_objects
    unlink_index = do.calls.index(("unlink", "/z/dst.bin", True))
    move_index = do.calls.index(("move", "/z/src.bin", "/z/dst.bin"))
    assert unlink_index < move_index


def test_mv_same_path_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    session = RecordingSession()
    fs = make_fs(session)
    monkeypatch.setattr(listing, "stat", _stat_map({"/z/a.bin": _file("/z/a.bin")}))
    fs.mv("/z/a.bin", "/z/a.bin")
    assert session.data_objects.calls == []


def test_mv_collection_renames(monkeypatch: pytest.MonkeyPatch) -> None:
    session = RecordingSession()
    fs = make_fs(session)
    monkeypatch.setattr(listing, "stat", _stat_map({"/z/dir": _dir("/z/dir")}))
    fs.mv("/z/dir", "/z/dir2", recursive=True)
    assert ("move", "/z/dir", "/z/dir2") in session.collections.calls


def test_mv_file_onto_existing_collection_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = RecordingSession()
    fs = make_fs(session)
    monkeypatch.setattr(
        listing,
        "stat",
        _stat_map({"/z/src.bin": _file("/z/src.bin"), "/z/dir": _dir("/z/dir")}),
    )
    fallback_calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "fsspec.spec.AbstractFileSystem.mv",
        lambda self, p1, p2, **kw: fallback_calls.append((p1, p2)),
    )
    fs.mv("/z/src.bin", "/z/dir")
    assert fallback_calls == [("/z/src.bin", "/z/dir")]
    assert session.data_objects.calls == []  # no native move, no unlink


def test_mv_collection_evicts_descendant_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = RecordingSession()
    fs = make_fs(session)
    monkeypatch.setattr(listing, "stat", _stat_map({"/z/dir": _dir("/z/dir")}))
    fs.dircache["/z/dir"] = []
    fs.dircache["/z/dir/sub"] = []  # stale old-path listing after the rename
    fs.dircache["/z/other"] = []
    fs.mv("/z/dir", "/z/dir2", recursive=True)
    assert "/z/dir" not in fs.dircache
    assert "/z/dir/sub" not in fs.dircache
    assert "/z/other" in fs.dircache


@pytest.mark.parametrize(
    ("recursive", "entries"),
    [
        (True, {"/z/dir": _dir("/z/dir"), "/z/dir2": _dir("/z/dir2")}),
        (False, {"/z/dir": _dir("/z/dir")}),
    ],
    ids=["dst-exists", "non-recursive"],
)
def test_mv_collection_falls_back_to_copy(
    monkeypatch: pytest.MonkeyPatch, recursive: bool, entries: dict[str, dict]
) -> None:
    """Cases a catalog rename cannot express delegate to fsspec's copy+rm mv."""
    session = RecordingSession()
    fs = make_fs(session)
    monkeypatch.setattr(listing, "stat", _stat_map(entries))
    fallback_calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "fsspec.spec.AbstractFileSystem.mv",
        lambda self, p1, p2, **kw: fallback_calls.append((p1, p2)),
    )
    fs.mv("/z/dir", "/z/dir2", recursive=recursive)
    assert fallback_calls == [("/z/dir", "/z/dir2")]
    assert all(call[0] != "move" for call in session.collections.calls)


def test_transaction_write_stages_then_commits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = RecordingSession()
    fs = make_fs(session)
    monkeypatch.setattr(listing, "stat", _stat_map({}))
    f = cast(
        DucktapeBufferedFile, fs.open("/z/home/rods/out.bin", "wb", autocommit=False)
    )
    cast(Any, f).write(b"payload")
    f.close()
    do = session.data_objects
    (open_call,) = [call for call in do.calls if call[0] == "open"]
    staging = open_call[1]
    assert staging.startswith("/z/home/rods/.out.bin.ducktape-tmp-")
    assert do.handle.written == b"payload"
    assert all(call[0] != "move" for call in do.calls)  # nothing published yet
    f.commit()
    assert ("move", staging, "/z/home/rods/out.bin") in do.calls


def test_transaction_discard_unlinks_staging(monkeypatch: pytest.MonkeyPatch) -> None:
    session = RecordingSession()
    fs = make_fs(session)
    monkeypatch.setattr(
        listing, "stat", lambda session, path: _file(path)
    )  # staging object exists
    f = cast(
        DucktapeBufferedFile, fs.open("/z/home/rods/out.bin", "wb", autocommit=False)
    )
    cast(Any, f).write(b"payload")
    f.close()
    do = session.data_objects
    (open_call,) = [call for call in do.calls if call[0] == "open"]
    staging = open_call[1]
    f.discard()
    assert ("unlink", staging, True) in do.calls
    assert all(call[0] != "move" for call in do.calls)


def test_write_close_invalidates_parent_listing() -> None:
    session = RecordingSession()
    fs = make_fs(session)
    f = fs.open("/z/home/rods/new.bin", "wb")
    cast(Any, f).write(b"x")
    # Re-cached between open and close (e.g. by a concurrent ls) — close must evict it.
    fs.dircache["/z/home/rods"] = [{"name": "stale"}]
    f.close()
    assert "/z/home/rods" not in fs.dircache


def test_empty_file_write_creates_object() -> None:
    session = RecordingSession()
    fs = make_fs(session)
    with fs.open("/z/home/rods/empty.bin", "wb"):
        pass
    do = session.data_objects
    assert ("open", "/z/home/rods/empty.bin", "w", False) in do.calls
    assert do.handle.written == b""
    assert do.handle.closed is True


def test_multi_chunk_write_appends_in_order() -> None:
    session = RecordingSession()
    fs = make_fs(session)
    block = 5
    payload = b"abcdefghijklmnopqrstuvwxyz"  # spans several blocks
    with fs.open("/z/home/rods/big.bin", "wb", block_size=block) as handle:
        written: Any = handle
        written.write(payload)
    do = session.data_objects
    assert do.handle.written == payload
    open_calls = [call for call in do.calls if call[0] == "open"]
    assert len(open_calls) == 1  # one handle reused across chunks
