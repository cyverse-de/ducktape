"""Catalog queries that turn iRODS collections into fsspec listing dicts.

Listings come from paged GenQuery against the catalog rather than from
`collection.data_objects`/`subcollections`, which fetch lazily per row and would issue
thousands of round trips for a large collection. GenQuery returns one row per *replica*,
so data objects are deduplicated by name (keeping the most recently modified replica).

The pure helpers (`dedup_replicas`, `file_info`, `dir_info`) are unit-tested with plain
dicts; the session-bound query functions are covered by the gated integration tests.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from typing import TYPE_CHECKING, Any

from irods.models import Collection, DataObject

from .errors import IrodsFileNotFoundError
from .paths import ROOT, base_name, parent_path

if TYPE_CHECKING:
    from irods.session import iRODSSession

InfoDict = dict[str, Any]


def _join(collection: str, name: str) -> str:
    return f"{collection.rstrip('/')}/{name}"


def dedup_replicas(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Collapse one-row-per-replica GenQuery output to one row per data object.

    Keeps the replica with the latest `modify_time`, which is the one whose size and
    timestamps best reflect the current object.
    """
    by_name: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = row["name"]
        current = by_name.get(name)
        if current is None or row["modify_time"] > current["modify_time"]:
            by_name[name] = dict(row)
    return list(by_name.values())


def file_info(collection: str, row: Mapping[str, Any]) -> InfoDict:
    """Build an fsspec info dict for a data object living in `collection`."""
    return {
        "name": _join(collection, row["name"]),
        "size": int(row["size"]),
        "type": "file",
        "created": row.get("create_time"),
        "modified": row.get("modify_time"),
    }


def dir_info(full_path: str, row: Mapping[str, Any] | None = None) -> InfoDict:
    """Build an fsspec info dict for a collection at its full logical path."""
    row = row or {}
    return {
        "name": full_path,
        "size": 0,
        "type": "directory",
        "created": row.get("create_time"),
        "modified": row.get("modify_time"),
    }


def _data_object_rows(
    session: iRODSSession, collection: str, page_size: int | None
) -> Iterator[dict[str, Any]]:
    query = session.query(
        DataObject.name,
        DataObject.size,
        DataObject.modify_time,
        DataObject.create_time,
    ).filter(Collection.name == collection)
    if page_size:
        query = query.limit(page_size)
    for row in query.get_results():
        yield {
            "name": row[DataObject.name],
            "size": row[DataObject.size],
            "modify_time": row[DataObject.modify_time],
            "create_time": row[DataObject.create_time],
        }


def _subcollection_rows(
    session: iRODSSession, collection: str, page_size: int | None
) -> Iterator[dict[str, Any]]:
    query = session.query(
        Collection.name,
        Collection.modify_time,
        Collection.create_time,
    ).filter(Collection.parent_name == collection)
    if page_size:
        query = query.limit(page_size)
    for row in query.get_results():
        yield {
            "name": row[Collection.name],
            "modify_time": row[Collection.modify_time],
            "create_time": row[Collection.create_time],
        }


def list_collection_children(
    session: iRODSSession, collection: str, page_size: int | None = None
) -> list[InfoDict]:
    """Return fsspec info dicts for the direct children of a collection."""
    files = dedup_replicas(_data_object_rows(session, collection, page_size))
    file_infos = [file_info(collection, row) for row in files]
    dir_infos = [
        dir_info(row["name"], row)
        for row in _subcollection_rows(session, collection, page_size)
    ]
    return file_infos + dir_infos


def _first_data_object(
    session: iRODSSession, collection: str, name: str
) -> dict[str, Any] | None:
    # Query.first() closes the GenQuery continuation eagerly; iterating get_results()
    # and returning early would leave the generator suspended to be closed by GC on a
    # possibly-dead connection (noisy and leaks the server-side query handle).
    row = (
        session.query(
            DataObject.name,
            DataObject.size,
            DataObject.modify_time,
            DataObject.create_time,
        )
        .filter(Collection.name == collection)
        .filter(DataObject.name == name)
        .first()
    )
    if row is None:
        return None
    return {
        "name": row[DataObject.name],
        "size": row[DataObject.size],
        "modify_time": row[DataObject.modify_time],
        "create_time": row[DataObject.create_time],
    }


def _collection_row(session: iRODSSession, path: str) -> dict[str, Any] | None:
    row = (
        session.query(Collection.name, Collection.modify_time, Collection.create_time)
        .filter(Collection.name == path)
        .first()
    )
    if row is None:
        return None
    return {
        "modify_time": row[Collection.modify_time],
        "create_time": row[Collection.create_time],
    }


def stat(session: iRODSSession, path: str) -> InfoDict:
    """Return the fsspec info dict for a single path, or raise if it does not exist.

    Does a targeted data-object lookup then a collection lookup (two cheap queries),
    avoiding a full parent listing just to describe one entry.
    """
    if path == ROOT:
        return dir_info(ROOT)

    parent = parent_path(path)
    name = base_name(path)
    row = _first_data_object(session, parent, name)
    if row is not None:
        return file_info(parent, row)

    collection = _collection_row(session, path)
    if collection is not None:
        return dir_info(path, collection)

    raise IrodsFileNotFoundError(path)
