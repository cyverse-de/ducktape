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
