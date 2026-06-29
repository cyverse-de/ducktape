from __future__ import annotations

import pytest

from ducktape.errors import IrodsPathError
from ducktape.paths import base_name, normalize_irods_path, parent_path


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("irods:///tempZone/home/rods/x.parquet", "/tempZone/home/rods/x.parquet"),
        ("/tempZone/home/rods/", "/tempZone/home/rods"),
        ("irods:///tempZone//home///rods", "/tempZone/home/rods"),
        ("irods:/tempZone/home/rods", "/tempZone/home/rods"),
        ("tempZone/home/rods", "/tempZone/home/rods"),
        ("irods:///", "/"),
        ("irods://", "/"),
        ("/", "/"),
        ("", "/"),
        (None, "/"),
    ],
    ids=[
        "full-url",
        "trailing-slash",
        "duplicate-slashes",
        "single-slash-scheme",
        "relative",
        "root-url-triple",
        "root-url-bare",
        "root-slash",
        "empty",
        "none",
    ],
)
def test_normalize_irods_path(raw: str | None, expected: str) -> None:
    assert normalize_irods_path(raw) == expected


@pytest.mark.parametrize(
    "raw",
    ["irods://tempZone/home/rods/x.parquet", "irods://somehost/tempZone/home"],
    ids=["zone-as-host", "explicit-host"],
)
def test_normalize_rejects_netloc(raw: str) -> None:
    with pytest.raises(IrodsPathError):
        normalize_irods_path(raw)


def test_normalize_rejects_foreign_scheme() -> None:
    with pytest.raises(IrodsPathError):
        normalize_irods_path("s3://bucket/key")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("irods:///tempZone/home/rods/x.parquet", "/tempZone/home/rods"),
        ("/tempZone", "/"),
        ("/", "/"),
    ],
    ids=["nested", "top-level", "root"],
)
def test_parent_path(raw: str, expected: str) -> None:
    assert parent_path(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("irods:///tempZone/home/rods/x.parquet", "x.parquet"),
        ("/tempZone", "tempZone"),
        ("/", ""),
    ],
    ids=["file", "top-level", "root"],
)
def test_base_name(raw: str, expected: str) -> None:
    assert base_name(raw) == expected
