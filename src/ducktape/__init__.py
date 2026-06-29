"""ducktape: an fsspec filesystem backend for iRODS.

Registered under the ``irods`` protocol via the ``fsspec.specs`` entry point, so
``fsspec.filesystem("irods", ...)`` and ``irods:///zone/...`` URLs resolve once the
package is installed. Call :func:`register` as a fallback for editable/dev installs
where entry points may not be discovered.
"""

from __future__ import annotations

from .errors import (
    DucktapeError,
    IrodsAuthError,
    IrodsFileNotFoundError,
    IrodsNotEmptyError,
    IrodsOperationError,
    IrodsPathError,
)
from .file import DucktapeBufferedFile
from .filesystem import DucktapeFileSystem

__all__ = [
    "DucktapeBufferedFile",
    "DucktapeError",
    "DucktapeFileSystem",
    "IrodsAuthError",
    "IrodsFileNotFoundError",
    "IrodsNotEmptyError",
    "IrodsOperationError",
    "IrodsPathError",
    "register",
]


def register() -> None:
    """Register the ``irods`` protocol with fsspec (fallback for dev installs)."""
    from fsspec import register_implementation

    register_implementation("irods", DucktapeFileSystem, clobber=True)
