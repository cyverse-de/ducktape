from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

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
