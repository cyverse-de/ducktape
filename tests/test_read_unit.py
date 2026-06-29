from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from irods.exception import HIERARCHY_ERROR

from ducktape import listing
from ducktape.file import DucktapeBufferedFile
from ducktape.filesystem import DucktapeFileSystem


class StubProvider:
    """Returns a sentinel session object; listing functions are stubbed in tests."""

    def __init__(self, session: object | None = None) -> None:
        self._session = session if session is not None else object()

    def session(self) -> Any:
        return self._session

    def close(self) -> None:
        pass


def make_fs(session: object | None = None) -> DucktapeFileSystem:
    return DucktapeFileSystem(
        host="irods.example.org",
        user="rods",
        password="secret",
        zone="tempZone",
        skip_instance_cache=True,
        session_provider=StubProvider(session),
    )


def test_info_delegates_to_stat(monkeypatch: pytest.MonkeyPatch) -> None:
    fs = make_fs()
    monkeypatch.setattr(
        listing,
        "stat",
        lambda session, path: {"name": path, "type": "file", "size": 3},
    )
    info = fs.info("irods:///tempZone/home/rods/x")
    assert info["name"] == "/tempZone/home/rods/x"
    assert info["type"] == "file"


def test_ls_directory_uses_children_and_caches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fs = make_fs()
    monkeypatch.setattr(
        listing,
        "stat",
        lambda session, path: {"name": path, "type": "directory", "size": 0},
    )
    children = [
        {"name": "/tempZone/home/rods/a", "type": "file", "size": 1},
        {"name": "/tempZone/home/rods/sub", "type": "directory", "size": 0},
    ]
    calls = {"count": 0}

    def fake_children(session: object, path: str, page_size: int | None) -> list[dict]:
        calls["count"] += 1
        return children

    monkeypatch.setattr(listing, "list_collection_children", fake_children)

    detailed = fs.ls("/tempZone/home/rods")
    assert detailed == children
    names = fs.ls("/tempZone/home/rods", detail=False)
    assert names == ["/tempZone/home/rods/a", "/tempZone/home/rods/sub"]
    assert calls["count"] == 1  # second ls served from dircache


def test_ls_on_file_returns_single_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    fs = make_fs()
    monkeypatch.setattr(
        listing,
        "stat",
        lambda session, path: {"name": path, "type": "file", "size": 9},
    )
    result = fs.ls("/tempZone/home/rods/x", detail=True)
    assert result == [{"name": "/tempZone/home/rods/x", "type": "file", "size": 9}]


def test_invalidate_cache_drops_path_and_parent() -> None:
    fs = make_fs()
    fs.dircache["/tempZone/home/rods"] = []
    fs.dircache["/tempZone/home"] = []
    fs.invalidate_cache("/tempZone/home/rods")
    assert "/tempZone/home/rods" not in fs.dircache
    assert "/tempZone/home" not in fs.dircache


class FakeHandle:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0
        self.closed = False

    def seek(self, pos: int) -> int:
        self._pos = pos
        return pos

    def read(self, length: int) -> bytes:
        chunk = self._data[self._pos : self._pos + length]
        self._pos += len(chunk)
        return chunk

    def close(self) -> None:
        self.closed = True


class FakeDataObjects:
    def __init__(self, handle: FakeHandle) -> None:
        self._handle = handle
        self.opened: list[tuple[str, str, object]] = []

    def open(self, path: str, mode: str, allow_redirect: object = None) -> FakeHandle:
        self.opened.append((path, mode, allow_redirect))
        return self._handle


class FakeSession:
    def __init__(self, handle: FakeHandle) -> None:
        self.data_objects = FakeDataObjects(handle)


def test_fetch_range_seeks_persistent_handle() -> None:
    payload = bytes(range(256))
    handle = FakeHandle(payload)
    fs = make_fs()
    f = DucktapeBufferedFile(fs, "/tempZone/home/rods/x", mode="rb", size=len(payload))
    f._handle = handle  # inject; avoid opening a real session

    assert f._fetch_range(10, 20) == payload[10:20]
    assert f._fetch_range(100, 110) == payload[100:110]

    f.close()
    assert handle.closed is True


def test_open_disables_redirect_by_default() -> None:
    session = FakeSession(FakeHandle(b"abc"))
    fs = make_fs(session=session)
    f = DucktapeBufferedFile(fs, "/z/x", mode="rb", size=3)
    assert f._fetch_range(0, 3) == b"abc"
    assert session.data_objects.opened == [("/z/x", "r", False)]


class RaceDetectingHandle:
    """Flags any overlap between a seek and its following read across threads.

    `_in_op` is set at seek and cleared at read; a second seek arriving while it is still set
    means two threads interleaved on the single shared file position. The sleep widens the
    window so an unguarded seek+read reliably trips the detector.
    """

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0
        self._in_op = False
        self.violations = 0
        self.closed = False

    def seek(self, pos: int) -> int:
        if self._in_op:
            self.violations += 1
        self._in_op = True
        time.sleep(0.0002)
        self._pos = pos
        return pos

    def read(self, length: int) -> bytes:
        chunk = self._data[self._pos : self._pos + length]
        self._pos += len(chunk)
        self._in_op = False
        return chunk

    def close(self) -> None:
        self.closed = True


def test_fetch_range_is_atomic_under_concurrency() -> None:
    payload = bytes(range(256)) * 16  # 4 KiB
    handle = RaceDetectingHandle(payload)
    fs = make_fs()
    f = DucktapeBufferedFile(fs, "/z/x", mode="rb", size=len(payload))
    f._handle = handle  # inject; avoid opening a real session

    ranges = [(i * 16, i * 16 + 16) for i in range(len(payload) // 16)] * 8

    def fetch(span: tuple[int, int]) -> tuple[int, bytes]:
        start, end = span
        return start, f._fetch_range(start, end)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(fetch, ranges))

    for start, chunk in results:
        assert chunk == payload[start : start + 16]
    assert handle.violations == 0


def test_concurrent_first_reads_open_handle_once() -> None:
    session = FakeSession(FakeHandle(bytes(range(256))))
    fs = make_fs(session=session)
    f = DucktapeBufferedFile(fs, "/z/x", mode="rb", size=256)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: f._fetch_range(0, 10), range(8)))

    assert len(session.data_objects.opened) == 1


class FlakyDataObjects:
    """Raises HIERARCHY_ERROR for the first `fail_times` opens, then returns the handle."""

    def __init__(self, handle: FakeHandle, fail_times: int) -> None:
        self._handle = handle
        self._fail_times = fail_times
        self.open_attempts = 0

    def open(self, path: str, mode: str, allow_redirect: object = None) -> FakeHandle:
        self.open_attempts += 1
        if self.open_attempts <= self._fail_times:
            raise HIERARCHY_ERROR("resource hierarchy race")
        return self._handle


class FlakySession:
    def __init__(self, handle: FakeHandle, fail_times: int) -> None:
        self.data_objects = FlakyDataObjects(handle, fail_times)


def make_retry_fs(session: object, retries: int) -> DucktapeFileSystem:
    return DucktapeFileSystem(
        host="irods.example.org",
        user="rods",
        password="secret",
        zone="tempZone",
        hierarchy_retries=retries,
        hierarchy_retry_backoff=0,  # keep the test instant
        skip_instance_cache=True,
        session_provider=StubProvider(session),
    )


def test_open_retries_on_hierarchy_error() -> None:
    session = FlakySession(FakeHandle(b"abc"), fail_times=2)
    fs = make_retry_fs(session, retries=3)
    f = DucktapeBufferedFile(fs, "/z/x", mode="rb", size=3)
    assert f._fetch_range(0, 3) == b"abc"
    assert session.data_objects.open_attempts == 3  # 2 failures + 1 success


def test_open_retries_exhausted_reraises() -> None:
    session = FlakySession(FakeHandle(b"abc"), fail_times=5)
    fs = make_retry_fs(session, retries=3)
    f = DucktapeBufferedFile(fs, "/z/x", mode="rb", size=3)
    with pytest.raises(HIERARCHY_ERROR):
        f._fetch_range(0, 3)
    assert session.data_objects.open_attempts == 4  # initial try + 3 retries


def test_open_retry_disabled() -> None:
    session = FlakySession(FakeHandle(b"abc"), fail_times=1)
    fs = make_retry_fs(session, retries=0)
    f = DucktapeBufferedFile(fs, "/z/x", mode="rb", size=3)
    with pytest.raises(HIERARCHY_ERROR):
        f._fetch_range(0, 3)
    assert session.data_objects.open_attempts == 1  # no retry


def test_open_honors_allow_redirect() -> None:
    session = FakeSession(FakeHandle(b"abc"))
    fs = DucktapeFileSystem(
        host="irods.example.org",
        user="rods",
        password="secret",
        zone="tempZone",
        allow_redirect=True,
        skip_instance_cache=True,
        session_provider=StubProvider(session),
    )
    f = DucktapeBufferedFile(fs, "/z/x", mode="rb", size=3)
    f._fetch_range(0, 1)
    assert session.data_objects.opened[0][2] is True
