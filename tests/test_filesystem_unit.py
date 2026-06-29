from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from ducktape import listing
from ducktape.auth import AuthConfig, SessionProvider
from ducktape.filesystem import DucktapeFileSystem

if TYPE_CHECKING:
    from irods.session import iRODSSession


class FakeProvider:
    """A SessionProvider that never touches the network (Phase 0 instantiation test)."""

    def __init__(self) -> None:
        self.closed = False

    def session(self) -> iRODSSession:
        raise AssertionError("no session should be created in this test")

    def close(self) -> None:
        self.closed = True


def make_fs() -> DucktapeFileSystem:
    return DucktapeFileSystem(
        host="irods.example.org",
        user="rods",
        password="secret",
        zone="tempZone",
        skip_instance_cache=True,
        session_provider=FakeProvider(),
    )


def test_instantiates_without_network() -> None:
    fs = make_fs()
    assert isinstance(fs.auth, AuthConfig)
    assert fs.auth.mode == "explicit"


def test_close_releases_provider() -> None:
    provider = FakeProvider()
    fs = DucktapeFileSystem(
        host="irods.example.org",
        user="rods",
        password="secret",
        zone="tempZone",
        skip_instance_cache=True,
        session_provider=provider,
    )
    fs.close()
    assert provider.closed is True


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("irods:///tempZone/home/rods/x.parquet", "/tempZone/home/rods/x.parquet"),
        ("/tempZone/home/rods/", "/tempZone/home/rods"),
    ],
    ids=["url", "trailing-slash"],
)
def test_strip_protocol(raw: str, expected: str) -> None:
    assert DucktapeFileSystem._strip_protocol(raw) == expected


def test_session_provider_protocol_accepts_fake() -> None:
    provider: SessionProvider = FakeProvider()
    assert provider.close() is None


class StubProvider:
    """Returns a sentinel session; listing functions are monkeypatched in walk/find tests."""

    def session(self) -> Any:
        return object()

    def close(self) -> None:
        pass


def make_walk_fs(monkeypatch: pytest.MonkeyPatch) -> DucktapeFileSystem:
    fs = DucktapeFileSystem(
        host="irods.example.org",
        user="rods",
        password="secret",
        zone="tempZone",
        skip_instance_cache=True,
        session_provider=StubProvider(),
    )
    files = [
        listing.file_info("/r", {"name": "a.txt", "size": 1}),
        listing.file_info("/r/sub", {"name": "b.txt", "size": 1}),
        listing.file_info("/r/sub/deep", {"name": "c.txt", "size": 1}),
    ]
    dirs = [listing.dir_info("/r/sub"), listing.dir_info("/r/sub/deep")]
    monkeypatch.setattr(
        listing, "stat", lambda s, p: {"name": p, "type": "directory", "size": 0}
    )
    monkeypatch.setattr(listing, "walk_data_objects", lambda s, root: list(files))
    monkeypatch.setattr(listing, "walk_collections", lambda s, root: list(dirs))
    return fs


def test_find_returns_all_files_sorted(monkeypatch: pytest.MonkeyPatch) -> None:
    fs = make_walk_fs(monkeypatch)
    assert fs.find("/r") == ["/r/a.txt", "/r/sub/b.txt", "/r/sub/deep/c.txt"]


def test_find_withdirs_includes_collections(monkeypatch: pytest.MonkeyPatch) -> None:
    fs = make_walk_fs(monkeypatch)
    result = fs.find("/r", withdirs=True)
    assert set(result) == {
        "/r",
        "/r/a.txt",
        "/r/sub",
        "/r/sub/b.txt",
        "/r/sub/deep",
        "/r/sub/deep/c.txt",
    }


def test_find_maxdepth_limits_descent(monkeypatch: pytest.MonkeyPatch) -> None:
    fs = make_walk_fs(monkeypatch)
    assert fs.find("/r", maxdepth=1) == ["/r/a.txt"]


def test_walk_buckets_by_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    fs = make_walk_fs(monkeypatch)
    tree = {root: (sorted(dirs), sorted(files)) for root, dirs, files in fs.walk("/r")}
    assert tree["/r"] == (["sub"], ["a.txt"])
    assert tree["/r/sub"] == (["deep"], ["b.txt"])
    assert tree["/r/sub/deep"] == ([], ["c.txt"])
