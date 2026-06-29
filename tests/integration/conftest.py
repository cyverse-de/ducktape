"""Fixtures for iRODS integration tests.

These tests require a reachable iRODS server. They are skipped automatically unless
`IRODS_TEST_HOST` is set. Configure the connection with these environment variables:

    IRODS_TEST_HOST        iRODS host (presence of this var enables the tests)
    IRODS_TEST_PORT        default 1247
    IRODS_TEST_USER        iRODS user name
    IRODS_TEST_PASSWORD    iRODS password
    IRODS_TEST_ZONE        iRODS zone
    IRODS_TEST_COLLECTION  a collection the user may write to (e.g. /tempZone/home/rods/ducktape-it)

Known noise — these runs may emit ``PytestUnraisableExceptionWarning`` (e.g. "Exception
ignored in: <irods.data_object.iRODSDataObjectFileRaw>", "Unable to send message"). This
is a python-irodsclient bug, not a ducktape one: ``iRODSDataObjectFileRaw.close()`` is not
idempotent, and the base io object's ``__del__`` re-invokes it at garbage-collection time.
When that GC happens after the session pool has been disconnected (process exit), the
second close sends on a dead socket and raises. The residual cases come from PRC handles
the *tests* create directly — the ``_seed_object`` writes via
``session.data_objects.open(..., "w")`` and PRC's internal parallel-transfer handles — not
from ducktape's ``DucktapeBufferedFile`` (which closes deterministically) or its catalog
queries (which close their GenQuery continuations eagerly via ``Query.first()``). The
warnings are harmless and do not affect results; we leave them visible rather than masking
genuine unraisable exceptions.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable, Iterator

import pytest

from ducktape.filesystem import DucktapeFileSystem

logger = logging.getLogger("ducktape.tests")


def remove_collection_quietly(fs: DucktapeFileSystem, collection: str) -> None:
    """Best-effort recursive cleanup of a test collection.

    CyVerse delete rules and catalog lag can make a recursive remove intermittently raise
    (e.g. CAT_COLLECTION_NOT_EMPTY) right after bulk seeding. Retry once after a short
    pause, then warn rather than failing a test whose assertions already passed.
    """
    for attempt in range(2):
        try:
            fs.session.collections.remove(collection, recurse=True, force=True)
            break
        except Exception as exc:  # noqa: BLE001 - teardown must not mask test results
            if attempt == 0:
                time.sleep(1.0)
                continue
            logger.warning("could not remove test collection %s: %s", collection, exc)
    fs.invalidate_cache()


@pytest.fixture
def collection_cleanup() -> Callable[[DucktapeFileSystem, str], None]:
    """Expose `remove_collection_quietly` to test modules without cross-imports."""
    return remove_collection_quietly


@pytest.fixture(scope="session")
def irods_config() -> dict[str, str | int]:
    host = os.environ.get("IRODS_TEST_HOST")
    if not host:
        pytest.skip("set IRODS_TEST_HOST to run iRODS integration tests")
    return {
        "host": host,
        "port": int(os.environ.get("IRODS_TEST_PORT", "1247")),
        "user": os.environ["IRODS_TEST_USER"],
        "password": os.environ["IRODS_TEST_PASSWORD"],
        "zone": os.environ["IRODS_TEST_ZONE"],
    }


@pytest.fixture(scope="session")
def base_collection() -> str:
    return os.environ.get(
        "IRODS_TEST_COLLECTION",
        f"/{os.environ.get('IRODS_TEST_ZONE', 'tempZone')}/home/"
        f"{os.environ.get('IRODS_TEST_USER', 'rods')}/ducktape-it",
    )


@pytest.fixture
def fs(irods_config: dict[str, str | int]) -> Iterator[DucktapeFileSystem]:
    filesystem = DucktapeFileSystem(
        host=str(irods_config["host"]),
        port=int(irods_config["port"]),
        user=str(irods_config["user"]),
        password=str(irods_config["password"]),
        zone=str(irods_config["zone"]),
        skip_instance_cache=True,
    )
    try:
        yield filesystem
    finally:
        filesystem.close()
