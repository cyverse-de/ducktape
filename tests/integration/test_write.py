"""iRODS write-path integration tests (gated by IRODS_TEST_HOST)."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Iterator
from typing import Any, cast

import pytest

from ducktape.filesystem import DucktapeFileSystem

Cleanup = Callable[[DucktapeFileSystem, str], None]


def _write_object(fs: DucktapeFileSystem, path: str, data: bytes) -> None:
    with fs.open(path, "wb") as handle:
        cast(Any, handle).write(data)


@pytest.fixture
def work_collection(
    fs: DucktapeFileSystem, base_collection: str, collection_cleanup: Cleanup
) -> Iterator[str]:
    collection = f"{base_collection}/write-{os.getpid()}"
    fs.makedirs(collection, exist_ok=True)
    try:
        yield collection
    finally:
        collection_cleanup(fs, collection)


def test_write_then_read_back(fs: DucktapeFileSystem, work_collection: str) -> None:
    path = f"{work_collection}/written.bin"
    payload = os.urandom(3 * 1024 * 1024)  # 3 MiB -> spans multiple write blocks
    _write_object(fs, path, payload)

    assert fs.info(path)["size"] == len(payload)
    with fs.open(path, "rb") as handle:
        assert cast(bytes, handle.read()) == payload


def test_mkdir_and_rmdir(fs: DucktapeFileSystem, work_collection: str) -> None:
    sub = f"{work_collection}/sub"
    fs.mkdir(sub)
    assert fs.isdir(sub)
    fs.rmdir(sub)
    assert not fs.exists(sub)


def test_rm_file_idempotent(fs: DucktapeFileSystem, work_collection: str) -> None:
    path = f"{work_collection}/temp.bin"
    _write_object(fs, path, b"data")
    assert fs.exists(path)
    fs.rm_file(path)
    assert not fs.exists(path)
    fs.rm_file(path)  # second delete must succeed silently


def test_rmdir_idempotent(fs: DucktapeFileSystem, work_collection: str) -> None:
    sub = f"{work_collection}/gone"
    fs.mkdir(sub)
    fs.rmdir(sub)
    assert not fs.exists(sub)
    fs.rmdir(sub)  # second rmdir of a missing collection must succeed silently


def test_special_character_name_roundtrips(
    fs: DucktapeFileSystem, work_collection: str
) -> None:
    """An object whose name contains '#'/'?' must be addressable end to end."""
    path = f"{work_collection}/run#2?v=1.bin"
    payload = b"special-name"
    _write_object(fs, path, payload)
    assert fs.exists(path)
    assert fs.info(path)["size"] == len(payload)
    with fs.open(path, "rb") as handle:
        assert cast(bytes, handle.read()) == payload
    assert path in set(fs.find(work_collection))


def test_du_reports_total_size(fs: DucktapeFileSystem, work_collection: str) -> None:
    _write_object(fs, f"{work_collection}/x.bin", b"abc")
    _write_object(fs, f"{work_collection}/y.bin", b"de")
    fs.invalidate_cache()
    assert fs.du(work_collection) == 5


def test_cp_file(fs: DucktapeFileSystem, work_collection: str) -> None:
    src = f"{work_collection}/src.bin"
    dst = f"{work_collection}/dst.bin"
    payload = b"copy me"
    _write_object(fs, src, payload)
    fs.cp_file(src, dst)
    with fs.open(dst, "rb") as handle:
        assert cast(bytes, handle.read()) == payload


def test_cp_file_overwrites_existing(
    fs: DucktapeFileSystem, work_collection: str
) -> None:
    src = f"{work_collection}/src.bin"
    dst = f"{work_collection}/dst.bin"
    _write_object(fs, dst, b"stale")
    _write_object(fs, src, b"fresh")
    # mirrors DuckDB's temp->target rename (fsspec mv): the copy must clobber an existing dst.
    fs.cp_file(src, dst)
    with fs.open(dst, "rb") as handle:
        assert cast(bytes, handle.read()) == b"fresh"


def test_mv_file(fs: DucktapeFileSystem, work_collection: str) -> None:
    src = f"{work_collection}/mv-src.bin"
    dst = f"{work_collection}/mv-dst.bin"
    payload = b"move me"
    _write_object(fs, src, payload)
    fs.mv(src, dst)
    assert not fs.exists(src)
    with fs.open(dst, "rb") as handle:
        assert cast(bytes, handle.read()) == payload


def test_mv_file_overwrites_existing(
    fs: DucktapeFileSystem, work_collection: str
) -> None:
    src = f"{work_collection}/mv-src.bin"
    dst = f"{work_collection}/mv-dst.bin"
    _write_object(fs, dst, b"stale")
    _write_object(fs, src, b"fresh")
    # mirrors DuckDB's temp->target rename: the move must clobber an existing dst.
    fs.mv(src, dst)
    assert not fs.exists(src)
    with fs.open(dst, "rb") as handle:
        assert cast(bytes, handle.read()) == b"fresh"


def test_mv_collection(fs: DucktapeFileSystem, work_collection: str) -> None:
    src = f"{work_collection}/mv-dir"
    dst = f"{work_collection}/mv-dir-renamed"
    fs.mkdir(src)
    _write_object(fs, f"{src}/inner.bin", b"inner")
    fs.mv(src, dst, recursive=True)
    assert not fs.exists(src)
    with fs.open(f"{dst}/inner.bin", "rb") as handle:
        assert cast(bytes, handle.read()) == b"inner"


def test_transaction_commit_publishes(
    fs: DucktapeFileSystem, work_collection: str
) -> None:
    path = f"{work_collection}/txn.bin"
    with fs.transaction:
        _write_object(fs, path, b"committed")
        # Inside the transaction the final object must not exist yet.
        assert not fs.exists(path)
    with fs.open(path, "rb") as handle:
        assert cast(bytes, handle.read()) == b"committed"


def test_transaction_rollback_leaves_nothing(
    fs: DucktapeFileSystem, work_collection: str
) -> None:
    path = f"{work_collection}/txn-rollback.bin"
    with pytest.raises(RuntimeError):
        with fs.transaction:
            _write_object(fs, path, b"never published")
            raise RuntimeError("abort")
    fs.invalidate_cache()
    assert not fs.exists(path)
    leftovers = [name for name in fs.ls(work_collection, detail=False) if "txn" in name]
    assert leftovers == []  # no staging orphan either


def test_put_file_parallel(
    fs: DucktapeFileSystem, work_collection: str, tmp_path: object
) -> None:
    local = os.path.join(str(tmp_path), "upload.bin")
    payload = os.urandom(64 * 2**20)  # 64 MiB -> exercises parallel put
    with open(local, "wb") as out:
        out.write(payload)

    remote = f"{work_collection}/uploaded.bin"
    fs.put_file(local, remote)

    with fs.open(remote, "rb") as handle:
        assert (
            hashlib.sha256(cast(bytes, handle.read())).digest()
            == hashlib.sha256(payload).digest()
        )
