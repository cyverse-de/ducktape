"""DuckDB round-trips over iRODS via the registered `irods://` filesystem (gated)."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator

import duckdb
import pytest

from ducktape.filesystem import DucktapeFileSystem

Cleanup = Callable[[DucktapeFileSystem, str], None]


@pytest.fixture
def work_collection(
    fs: DucktapeFileSystem, base_collection: str, collection_cleanup: Cleanup
) -> Iterator[str]:
    collection = f"{base_collection}/duckdb-{os.getpid()}"
    fs.session.collections.create(collection, recurse=True)
    try:
        yield collection
    finally:
        collection_cleanup(fs, collection)


def test_read_parquet_over_irods(
    fs: DucktapeFileSystem, work_collection: str, tmp_path: object
) -> None:
    local_parquet = os.path.join(str(tmp_path), "data.parquet")
    con = duckdb.connect()
    con.sql(
        "COPY (SELECT range AS id, range * 2 AS doubled FROM range(1000)) "
        f"TO '{local_parquet}' (FORMAT PARQUET)"
    )

    remote = f"{work_collection}/data.parquet"
    fs.session.data_objects.put(local_parquet, remote)

    con.register_filesystem(fs)
    rows = con.sql(
        f"SELECT count(*) AS n, sum(doubled) AS total FROM read_parquet('irods://{remote}')"
    ).fetchone()
    assert rows is not None
    assert rows[0] == 1000
    assert rows[1] == sum(i * 2 for i in range(1000))


def test_copy_to_parquet_over_irods(
    fs: DucktapeFileSystem, work_collection: str
) -> None:
    remote = f"{work_collection}/out.parquet"
    con = duckdb.connect()
    con.register_filesystem(fs)
    con.sql(
        "COPY (SELECT range AS id FROM range(500)) "
        f"TO 'irods://{remote}' (FORMAT PARQUET)"
    )

    rows = con.sql(
        f"SELECT count(*) AS n, sum(id) AS total FROM read_parquet('irods://{remote}')"
    ).fetchone()
    assert rows is not None
    assert rows[0] == 500
    assert rows[1] == sum(range(500))
