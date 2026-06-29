"""iRODS read-path integration tests (gated by IRODS_TEST_HOST).

Test data is seeded directly through the PRC session (the ducktape write path lands in
Phase 2), then exercised through the ducktape filesystem.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import cast

import pytest

from ducktape import listing
from ducktape.file import DucktapeBufferedFile
from ducktape.filesystem import DucktapeFileSystem

Cleanup = Callable[[DucktapeFileSystem, str], None]


def _seed_object(fs: DucktapeFileSystem, path: str, data: bytes) -> None:
    with fs.session.data_objects.open(path, "w") as handle:
        handle.write(data)


@pytest.fixture
def work_collection(
    fs: DucktapeFileSystem, base_collection: str, collection_cleanup: Cleanup
) -> Iterator[str]:
    collection = f"{base_collection}/case-{os.getpid()}"
    fs.session.collections.create(collection, recurse=True)
    try:
        yield collection
    finally:
        collection_cleanup(fs, collection)


def test_ls_lists_objects_and_subcollections(
    fs: DucktapeFileSystem, work_collection: str
) -> None:
    _seed_object(fs, f"{work_collection}/a.bin", b"alpha")
    _seed_object(fs, f"{work_collection}/b.bin", b"beta")
    fs.session.collections.create(f"{work_collection}/sub", recurse=True)
    fs.invalidate_cache(work_collection)

    listing = {entry["name"]: entry for entry in fs.ls(work_collection)}
    assert listing[f"{work_collection}/a.bin"]["type"] == "file"
    assert listing[f"{work_collection}/a.bin"]["size"] == len(b"alpha")
    assert listing[f"{work_collection}/sub"]["type"] == "directory"


def test_info_file_and_directory(fs: DucktapeFileSystem, work_collection: str) -> None:
    _seed_object(fs, f"{work_collection}/a.bin", b"alpha")
    assert fs.info(f"{work_collection}/a.bin")["type"] == "file"
    assert fs.info(work_collection)["type"] == "directory"
    assert fs.exists(f"{work_collection}/a.bin")
    assert not fs.exists(f"{work_collection}/missing.bin")


def test_read_ranges(fs: DucktapeFileSystem, work_collection: str) -> None:
    payload = bytes(range(256)) * 64  # 16 KiB
    path = f"{work_collection}/ranges.bin"
    _seed_object(fs, path, payload)

    with fs.open(path, "rb") as handle:
        assert handle.read(10) == payload[:10]
        handle.seek(1000)
        assert handle.read(50) == payload[1000:1050]
        handle.seek(0)
        assert handle.read() == payload


def test_get_file_parallel(
    fs: DucktapeFileSystem, work_collection: str, tmp_path: object
) -> None:
    size = 64 * 2**20  # 64 MiB -> exercises PRC parallel transfer
    payload = os.urandom(size)
    path = f"{work_collection}/large.bin"
    _seed_object(fs, path, payload)

    local = os.path.join(str(tmp_path), "large.bin")
    fs.get_file(path, local)
    with open(local, "rb") as downloaded:
        assert (
            hashlib.sha256(downloaded.read()).digest()
            == hashlib.sha256(payload).digest()
        )


def test_paged_listing(fs: DucktapeFileSystem, work_collection: str) -> None:
    """Verify ls returns every child and GenQuery continuation pages correctly.

    Seeds enough objects to span several pages (with a deliberately small page size)
    rather than thousands; the goal is to exercise the paging loop, not to benchmark.
    Set IRODS_TEST_LISTING_COUNT higher to test real large-collection scale.
    """
    count = int(os.environ.get("IRODS_TEST_LISTING_COUNT", "60"))
    for index in range(count):
        _seed_object(fs, f"{work_collection}/obj-{index:05d}.bin", b"x")
    fs.invalidate_cache(work_collection)

    names = fs.ls(work_collection, detail=False)
    assert len(names) == count

    # Force GenQuery continuation across multiple pages with a small page size.
    paged = listing.list_collection_children(fs.session, work_collection, page_size=10)
    assert len({entry["name"] for entry in paged}) == count


def test_concurrent_reads_checksum(
    fs: DucktapeFileSystem, work_collection: str
) -> None:
    """Open many files and read them concurrently; verifies PRC pool thread-safety."""
    payloads = {
        f"{work_collection}/c-{index:02d}.bin": os.urandom(1 << 16)
        for index in range(12)
    }
    for path, data in payloads.items():
        _seed_object(fs, path, data)

    def read_all(path: str) -> bytes:
        with fs.open(path, "rb") as handle:
            return cast(bytes, handle.read())

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = dict(zip(payloads, pool.map(read_all, payloads)))

    for path, data in payloads.items():
        assert results[path] == data


def test_concurrent_ranges_one_handle(
    fs: DucktapeFileSystem, work_collection: str
) -> None:
    """Parallel range reads against a SINGLE file object must not corrupt each other.

    DuckDB reads one Parquet file from several threads; the per-file lock makes the shared
    handle's seek()+read() atomic. Exercises that path directly against a live handle.
    """
    chunk = 4096
    payload = os.urandom(64 * chunk)
    path = f"{work_collection}/single.bin"
    _seed_object(fs, path, payload)

    with fs.open(path, "rb") as raw:
        handle = cast(DucktapeBufferedFile, raw)
        spans = [(i * chunk, (i + 1) * chunk) for i in range(len(payload) // chunk)] * 4

        def read_span(span: tuple[int, int]) -> tuple[int, bytes]:
            start, end = span
            return start, handle._fetch_range(start, end)

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(read_span, spans))

    for start, data in results:
        assert data == payload[start : start + chunk]


def test_find_and_glob_recursive(fs: DucktapeFileSystem, work_collection: str) -> None:
    """find() and glob('**') return the whole subtree via the recursive GenQuery path."""
    _seed_object(fs, f"{work_collection}/top.parquet", b"x")
    fs.session.collections.create(f"{work_collection}/sub", recurse=True)
    _seed_object(fs, f"{work_collection}/sub/nested.parquet", b"y")
    fs.invalidate_cache()

    found = set(fs.find(work_collection))
    assert found == {
        f"{work_collection}/top.parquet",
        f"{work_collection}/sub/nested.parquet",
    }

    globbed = set(fs.glob(f"{work_collection}/**/*.parquet"))
    assert f"{work_collection}/sub/nested.parquet" in globbed
    assert f"{work_collection}/top.parquet" in globbed
