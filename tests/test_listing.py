from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

import pytest
from irods.models import Collection, DataObject

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


class _FakeQuery:
    """Returns a fixed row set regardless of filters, modelling iRODS LIKE over-matching.

    Real GenQuery would prune by collection; here every query returns the full superset so
    the tests assert that the Python prefix post-filter trims sibling over-matches.
    """

    def __init__(self, rows: list[dict[Any, Any]]) -> None:
        self._rows = rows

    def filter(self, *conditions: Any) -> _FakeQuery:
        return self

    def get_results(self) -> list[dict[Any, Any]]:
        return list(self._rows)


class _FakeSubtreeSession:
    def __init__(
        self, do_rows: list[dict[Any, Any]], coll_rows: list[dict[Any, Any]]
    ) -> None:
        self._do_rows = do_rows
        self._coll_rows = coll_rows

    def query(self, *columns: Any) -> _FakeQuery:
        if any(column is DataObject.name for column in columns):
            return _FakeQuery(self._do_rows)
        return _FakeQuery(self._coll_rows)


def _do_row(collection: str, name: str) -> dict[Any, Any]:
    return {
        Collection.name: collection,
        DataObject.name: name,
        DataObject.size: 5,
        DataObject.modify_time: DT1,
        DataObject.create_time: DT0,
    }


def _coll_row(name: str) -> dict[Any, Any]:
    return {
        Collection.name: name,
        Collection.modify_time: DT1,
        Collection.create_time: DT0,
    }


def test_walk_data_objects_excludes_sibling_overmatch() -> None:
    session = cast(
        "iRODSSession",
        _FakeSubtreeSession(
            do_rows=[
                _do_row("/p", "a.bin"),
                _do_row("/p/sub", "b.bin"),
                _do_row("/p_x", "sibling.bin"),  # LIKE '/p/%' must not leak this in
            ],
            coll_rows=[],
        ),
    )
    names = {info["name"] for info in listing.walk_data_objects(session, "/p")}
    assert names == {"/p/a.bin", "/p/sub/b.bin"}


def test_walk_collections_excludes_root_and_siblings() -> None:
    session = cast(
        "iRODSSession",
        _FakeSubtreeSession(
            do_rows=[],
            coll_rows=[_coll_row("/p"), _coll_row("/p/sub"), _coll_row("/p_x")],
        ),
    )
    infos = listing.walk_collections(session, "/p")
    assert {info["name"] for info in infos} == {"/p/sub"}
    assert all(info["type"] == "directory" for info in infos)


def test_walk_data_objects_dedups_replicas_by_path() -> None:
    rows = [_do_row("/p", "a.bin"), _do_row("/p", "a.bin")]  # two replicas
    rows[1][DataObject.modify_time] = DT2
    session = cast("iRODSSession", _FakeSubtreeSession(do_rows=rows, coll_rows=[]))
    infos = listing.walk_data_objects(session, "/p")
    assert len(infos) == 1
    assert infos[0]["modified"] == DT2
