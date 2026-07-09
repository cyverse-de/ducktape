from __future__ import annotations

from typing import Any

import pytest

from ducktape.auth import DEFAULT_ENV_FILE, AuthConfig, _make_session, resolve_auth
from ducktape.errors import IrodsAuthError


def test_resolve_auth_explicit() -> None:
    config = resolve_auth(
        {
            "host": "irods.example.org",
            "port": 1247,
            "user": "rods",
            "password": "secret",
            "zone": "tempZone",
        },
        env={},
    )
    assert config == AuthConfig(
        mode="explicit",
        host="irods.example.org",
        port=1247,
        user="rods",
        zone="tempZone",
        password="secret",
    )


def test_resolve_auth_password_falls_back_to_env() -> None:
    config = resolve_auth(
        {"host": "irods.example.org", "user": "rods", "zone": "tempZone"},
        env={"IRODS_PASSWORD": "from-env"},
    )
    assert config.password == "from-env"


@pytest.mark.parametrize(
    ("storage_options", "env", "expected_env_file"),
    [
        ({}, {}, DEFAULT_ENV_FILE),
        ({}, {"IRODS_ENVIRONMENT_FILE": "/etc/irods/env.json"}, "/etc/irods/env.json"),
        ({"irods_env_file": "/opt/env.json"}, {}, "/opt/env.json"),
        (
            {"irods_env_file": "/opt/env.json"},
            {"IRODS_ENVIRONMENT_FILE": "/etc/irods/env.json"},
            "/opt/env.json",
        ),
    ],
    ids=["default", "from-env", "from-option", "option-overrides-env"],
)
def test_resolve_auth_env_file(
    storage_options: dict[str, str], env: dict[str, str], expected_env_file: str
) -> None:
    config = resolve_auth(storage_options, env=env)
    assert config.mode == "env_file"
    assert config.env_file == expected_env_file


@pytest.mark.parametrize(
    "storage_options",
    [
        {"host": "irods.example.org"},
        {"host": "irods.example.org", "user": "rods"},
        {"host": "irods.example.org", "user": "rods", "zone": "tempZone"},
    ],
    ids=["missing-user-zone-password", "missing-zone-password", "missing-password"],
)
def test_resolve_auth_explicit_missing_fields(storage_options: dict[str, str]) -> None:
    with pytest.raises(IrodsAuthError):
        resolve_auth(storage_options, env={})


_EXPLICIT = {
    "host": "irods.example.org",
    "user": "rods",
    "password": "secret",
    "zone": "tempZone",
}


@pytest.mark.parametrize(
    ("port", "expected"),
    [("1247", 1247), (1247, 1247), ("abc", None), (0, None), (65536, None), (-1, None)],
    ids=["string-ok", "int-ok", "non-numeric", "zero", "too-high", "negative"],
)
def test_resolve_auth_validates_port(port: object, expected: int | None) -> None:
    options = {**_EXPLICIT, "port": port}
    if expected is None:
        with pytest.raises(IrodsAuthError):
            resolve_auth(options, env={})
    else:
        assert resolve_auth(options, env={}).port == expected


def test_password_absent_from_repr() -> None:
    config = resolve_auth(
        {
            "host": "irods.example.org",
            "user": "rods",
            "password": "super-secret",
            "zone": "tempZone",
        },
        env={},
    )
    assert "super-secret" not in repr(config)


def test_resolve_auth_carries_connection_options() -> None:
    options = {"client_server_policy": "CS_NEG_REQUIRE", "ssl_verify_server": "cert"}
    explicit = resolve_auth(
        {
            "host": "irods.example.org",
            "user": "rods",
            "password": "secret",
            "zone": "tempZone",
            "connection_options": options,
        },
        env={},
    )
    assert explicit.connection_options == options
    env_file = resolve_auth({"connection_options": options}, env={})
    assert env_file.mode == "env_file"
    assert env_file.connection_options == options


def test_connection_options_absent_from_repr() -> None:
    config = resolve_auth(
        {"connection_options": {"ssl_verify_server": "leaked?"}}, env={}
    )
    assert "leaked?" not in repr(config)


class _FakeSession:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


@pytest.fixture
def capture_session(monkeypatch: pytest.MonkeyPatch) -> type[_FakeSession]:
    import irods.session

    monkeypatch.setattr(irods.session, "iRODSSession", _FakeSession)
    return _FakeSession


def test_make_session_forwards_connection_options_explicit(
    capture_session: type[_FakeSession],
) -> None:
    config = AuthConfig(
        mode="explicit",
        host="irods.example.org",
        port=1247,
        user="rods",
        zone="tempZone",
        password="secret",
        connection_options={"client_server_policy": "CS_NEG_REQUIRE"},
    )
    session: Any = _make_session(config)
    assert session.kwargs["host"] == "irods.example.org"
    assert session.kwargs["client_server_policy"] == "CS_NEG_REQUIRE"


def test_make_session_forwards_connection_options_env_file(
    capture_session: type[_FakeSession],
) -> None:
    config = AuthConfig(
        mode="env_file",
        env_file="/opt/env.json",
        connection_options={"ssl_verify_server": "cert"},
    )
    session: Any = _make_session(config)
    assert session.kwargs["ssl_verify_server"] == "cert"
    assert session.kwargs["irods_env_file"].endswith("/opt/env.json")
