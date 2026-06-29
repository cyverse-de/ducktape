"""Typed exceptions for the ducktape iRODS filesystem.

Each error subclasses both `DucktapeError` (so callers can catch everything from
this package) and the stdlib exception that fsspec and DuckDB expect for a given
failure mode (e.g. `FileNotFoundError`). Callers and tests match on type, never on
message text. PRC errors are wrapped with `raise ... from exc` so the original cause
stays reachable via `__cause__`.
"""

from __future__ import annotations


class DucktapeError(Exception):
    """Base class for all errors raised by ducktape."""


class IrodsPathError(DucktapeError, ValueError):
    """Raised when an iRODS path or URL is malformed or ambiguous."""


class IrodsAuthError(DucktapeError, ValueError):
    """Raised when iRODS connection credentials are missing or invalid."""


class IrodsFileNotFoundError(DucktapeError, FileNotFoundError):
    """Raised when a data object or collection does not exist."""


class IrodsNotEmptyError(DucktapeError, OSError):
    """Raised when removing a non-empty collection without recursion."""


class IrodsOperationError(DucktapeError, OSError):
    """Raised when an iRODS operation fails for a reason without a more specific type."""
