"""iRODS authentication resolution and session provisioning.

`resolve_auth` is a pure function that merges fsspec `storage_options` with environment
fallbacks into a validated `AuthConfig`; it performs no I/O so it is exhaustively
unit-testable. `SessionProvider` is the seam through which the filesystem obtains a live
`iRODSSession`; isolating it here means the thread-safety strategy (single shared session
vs. per-thread sessions) can change without touching the filesystem code.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Protocol

from .errors import IrodsAuthError

if TYPE_CHECKING:
    from irods.session import iRODSSession

DEFAULT_PORT = 1247
DEFAULT_ENV_FILE = "~/.irods/irods_environment.json"


@dataclass(frozen=True)
class AuthConfig:
    """Resolved connection configuration for an iRODS session.

    In `explicit` mode all of host/port/user/zone/password are set from
    `storage_options`/env. In `env_file` mode the standard iRODS environment file
    supplies everything (PRC reads `.irodsA`/PAM/GSI from it). The password is excluded
    from `repr` so the secret never lands in a log line.
    """

    mode: Literal["explicit", "env_file"]
    host: str | None = None
    port: int = DEFAULT_PORT
    user: str | None = None
    zone: str | None = None
    password: str | None = field(default=None, repr=False)
    env_file: str | None = None


def resolve_auth(
    storage_options: Mapping[str, Any], env: Mapping[str, str]
) -> AuthConfig:
    """Resolve an `AuthConfig` from fsspec storage options and environment fallbacks.

    Presence of `host` selects explicit mode; otherwise the iRODS environment file is
    used. The password falls back to the `IRODS_PASSWORD` env var so secrets need not be
    passed as visible options, and the env-file path falls back to
    `IRODS_ENVIRONMENT_FILE` then the iRODS default.
    """
    if "host" in storage_options:
        password = storage_options.get("password") or env.get("IRODS_PASSWORD")
        config = AuthConfig(
            mode="explicit",
            host=storage_options.get("host"),
            port=int(storage_options.get("port", DEFAULT_PORT)),
            user=storage_options.get("user"),
            zone=storage_options.get("zone"),
            password=password,
        )
        missing = [
            name
            for name in ("host", "user", "zone", "password")
            if not getattr(config, name)
        ]
        if missing:
            raise IrodsAuthError(
                "explicit iRODS auth is missing required values: "
                + ", ".join(missing)
                + " (password may be supplied via the IRODS_PASSWORD env var)"
            )
        return config

    env_file = (
        storage_options.get("irods_env_file")
        or env.get("IRODS_ENVIRONMENT_FILE")
        or DEFAULT_ENV_FILE
    )
    return AuthConfig(mode="env_file", env_file=env_file)


def _make_session(config: AuthConfig) -> iRODSSession:
    from irods.session import iRODSSession

    if config.mode == "explicit":
        return iRODSSession(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            zone=config.zone,
        )
    assert config.env_file is not None
    return iRODSSession(irods_env_file=os.path.expanduser(config.env_file))


class SessionProvider(Protocol):
    """Supplies a live iRODS session and releases it on close."""

    def session(self) -> iRODSSession: ...

    def close(self) -> None: ...


class SingleSessionProvider:
    """Lazily builds one shared `iRODSSession` and reuses it for the FS instance.

    fsspec caches filesystem instances by their arguments, so one instance (and thus one
    session, with its own connection pool) corresponds to one distinct connection config.
    Construction is deferred until first use so `fsspec.filesystem("irods", ...)` can be
    created without immediately reaching the network.
    """

    def __init__(self, config: AuthConfig) -> None:
        self._config = config
        self._session: iRODSSession | None = None
        self._lock = threading.RLock()

    def session(self) -> iRODSSession:
        with self._lock:
            if self._session is None:
                self._session = _make_session(self._config)
            return self._session

    def close(self) -> None:
        with self._lock:
            if self._session is not None:
                self._session.cleanup()
                self._session = None
