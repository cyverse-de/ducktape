"""Pure functions for translating fsspec/`irods://` paths to iRODS logical paths.

iRODS logical paths are absolute and POSIX-like, rooted at the zone
(`/tempZone/home/rods/x.parquet`). There is no host component in the path — the host
is connection configuration, not part of the path — so the canonical URL form uses an
empty authority and three slashes: `irods:///tempZone/home/rods/x.parquet`.

Keeping this logic pure (no session, no I/O) makes it exhaustively unit-testable and
keeps fsspec's caching layer keyed on normalized paths.
"""

from __future__ import annotations

import logging

from .errors import IrodsPathError

logger = logging.getLogger("ducktape")

PROTOCOL = "irods"
ROOT = "/"


def normalize_irods_path(raw: str | None) -> str:
    """Normalize a single path or `irods://` URL to an absolute iRODS logical path.

    Rejects a non-empty URL authority (`irods://host/...`): the host belongs in
    `storage_options`, and treating the first segment as either a host or a zone is the
    classic S3 "bucket or host" footgun. Collapses duplicate slashes and strips trailing
    slashes; `irods:///`, `irods://`, `/`, and `""` all map to the root `/`.

    The path is parsed by hand rather than with `urllib.parse.urlsplit` because iRODS data
    object names legitimately contain `#` and `?`, which a URL parser would strip as a
    fragment/query and silently corrupt the path.
    """
    if raw is None or raw == "":
        return ROOT

    rest = raw
    scheme, sep, after = raw.partition(":")
    if sep and scheme and "/" not in scheme:
        if scheme != PROTOCOL:
            raise IrodsPathError(
                f"unsupported scheme {scheme!r}; expected {PROTOCOL!r} or a bare path"
            )
        rest = after
        # Strip an authority introduced by "//"; only the empty authority is allowed.
        if rest.startswith("//"):
            authority, slash, tail = rest[2:].partition("/")
            if authority:
                raise IrodsPathError(
                    f"unexpected host {authority!r} in iRODS path; the host belongs in "
                    f"storage_options — use {PROTOCOL}:///<zone>/... (three slashes)"
                )
            rest = slash + tail

    if rest and not rest.startswith("/"):
        logger.debug("normalizing relative iRODS path %r to absolute", raw)

    segments = [segment for segment in rest.split("/") if segment]
    # iRODS logical paths are literal: "." and ".." are not parent/self traversal but
    # would be treated as ordinary collection names. Reject them so a path like
    # irods:///zone/../other can never be silently misinterpreted by the server.
    if any(segment in (".", "..") for segment in segments):
        raise IrodsPathError(
            f"iRODS path may not contain '.' or '..' segments: {raw!r}"
        )
    if not segments:
        return ROOT
    return "/" + "/".join(segments)


def parent_path(path: str) -> str:
    """Return the parent collection of an iRODS logical path (`/` for the root)."""
    normalized = normalize_irods_path(path)
    if normalized == ROOT:
        return ROOT
    head, _, _ = normalized.rpartition("/")
    return head or ROOT


def base_name(path: str) -> str:
    """Return the final path component (empty string for the root)."""
    normalized = normalize_irods_path(path)
    if normalized == ROOT:
        return ""
    return normalized.rpartition("/")[2]
