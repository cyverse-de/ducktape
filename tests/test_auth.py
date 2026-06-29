from __future__ import annotations

import pytest

from ducktape.auth import DEFAULT_ENV_FILE, AuthConfig, resolve_auth
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
