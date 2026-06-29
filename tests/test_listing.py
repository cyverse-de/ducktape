from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, cast

import pytest

from ducktape import listing
from ducktape.errors import IrodsFileNotFoundError

if TYPE_CHECKING:
    from irods.session import iRODSSession

SESSION = cast("iRODSSession", object())

DT0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
DT1 = datetime(2021, 6, 1, tzinfo=timezone.utc)
DT2 = datetime(2022, 9, 1, tzinfo=timezone.utc)


def test_dedup_replicas_keeps_latest() -> None:
    rows = [
        {"name": "a.parquet", "size": 10, "modify_time": DT1, "create_time": DT0},
        {"name": "a.parquet", "size": 10, "modify_time": DT2, "create_time": DT0},
        {"name": "b.csv", "size": 5, "modify_time": DT1, "create_time": DT0},
    ]
    result = {row["name"]: row for row in listing.dedup_replicas(rows)}
    assert set(result) == {"a.parquet", "b.csv"}
    assert result["a.parquet"]["modify_time"] == DT2


def test_file_info_shape() -> None:
    info = listing.file_info(
        "/z/home/rods",
        {"name": "x.parquet", "size": 42, "modify_time": DT2, "create_time": DT0},
    )
    assert info == {
        "name": "/z/home/rods/x.parquet",
        "size": 42,
        "type": "file",
        "created": DT0,
        "modified": DT2,
    }


def test_file_info_joins_from_root() -> None:
    info = listing.file_info("/", {"name": "x", "size": 1})
    assert info["name"] == "/x"


def test_dir_info_shape() -> None:
    info = listing.dir_info(
        "/z/home/rods/sub", {"modify_time": DT1, "create_time": DT0}
    )
    assert info["name"] == "/z/home/rods/sub"
    assert info["type"] == "directory"
    assert info["size"] == 0


def test_list_collection_children(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        listing,
        "_data_object_rows",
        lambda session, collection, page_size: iter(
            [
                {
                    "name": "a.parquet",
                    "size": 10,
                    "modify_time": DT1,
                    "create_time": DT0,
                },
                {
                    "name": "a.parquet",
                    "size": 10,
                    "modify_time": DT2,
                    "create_time": DT0,
                },
                {"name": "b.csv", "size": 5, "modify_time": DT1, "create_time": DT0},
            ]
        ),
    )
    monkeypatch.setattr(
        listing,
        "_subcollection_rows",
        lambda session, collection, page_size: iter(
            [{"name": "/z/home/rods/sub", "modify_time": DT1, "create_time": DT0}]
        ),
    )

    result = listing.list_collection_children(SESSION, "/z/home/rods")

    by_name = {entry["name"]: entry for entry in result}
    assert set(by_name) == {
        "/z/home/rods/a.parquet",
        "/z/home/rods/b.csv",
        "/z/home/rods/sub",
    }
    assert by_name["/z/home/rods/a.parquet"]["modified"] == DT2
    assert by_name["/z/home/rods/sub"]["type"] == "directory"


def test_stat_file(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        listing,
        "_first_data_object",
        lambda session, collection, name: {
            "name": "x.parquet",
            "size": 7,
            "modify_time": DT2,
            "create_time": DT0,
        },
    )
    info = listing.stat(SESSION, "/z/home/rods/x.parquet")
    assert info["type"] == "file"
    assert info["name"] == "/z/home/rods/x.parquet"
    assert info["size"] == 7


def test_stat_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        listing, "_first_data_object", lambda session, collection, name: None
    )
    monkeypatch.setattr(
        listing,
        "_collection_row",
        lambda session, path: {"modify_time": DT1, "create_time": DT0},
    )
    info = listing.stat(SESSION, "/z/home/rods")
    assert info["type"] == "directory"
    assert info["name"] == "/z/home/rods"


def test_stat_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        listing, "_first_data_object", lambda session, collection, name: None
    )
    monkeypatch.setattr(listing, "_collection_row", lambda session, path: None)
    with pytest.raises(IrodsFileNotFoundError):
        listing.stat(SESSION, "/z/home/rods/missing")


def test_stat_root_is_directory() -> None:
    info = listing.stat(SESSION, "/")
    assert info == {
        "name": "/",
        "size": 0,
        "type": "directory",
        "created": None,
        "modified": None,
    }
