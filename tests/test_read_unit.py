from __future__ import annotations

from typing import Any

import pytest

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
