# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Before 1.0.0, minor-version bumps may include breaking API changes.

## [0.3.0] - 2026-07-09

### Added

- Native `mv` via iRODS catalog rename (`data_objects.move`/`collections.move`) instead of
  fsspec's server-side copy + delete — constant-time regardless of object size. Falls back
  to copy + delete for bounded-depth moves and for a collection moved onto an existing
  destination.
- Transactional writes: `autocommit=False` (e.g. inside `with fs.transaction:`) streams to
  a hidden staging object in the same collection; commit renames it over the target,
  rollback unlinks it. Previously the flag was silently ignored and rollback was a no-op.
- Port validation in `resolve_auth`: a non-numeric or out-of-range port now raises
  `IrodsAuthError` at construction instead of failing at connect time.
- `readme` and `license` metadata in `pyproject.toml`.

### Fixed

- `exists()` no longer treats network/auth failures as "does not exist" (fsspec's default
  swallows every exception), so idempotent `rm`/`rmdir` can no longer silently skip the
  delete when the existence probe fails; the probe error now propagates.
- All PRC exceptions are now translated to ducktape's typed errors on the read/listing
  path (`info`/`ls`/`walk`/`find`), on data-object opens, and in `rm_file` — previously
  raw `irods.exception` types could leak through. Exhausted `HIERARCHY_ERROR` retries now
  surface as `IrodsOperationError` (cause preserved).
- `put_file(..., mode="create")` raises `FileExistsError` on an existing target instead of
  silently overwriting; `open(path, "xb")` likewise enforces exclusive create.
- `open(collection, "rb")` raises `IsADirectoryError` instead of silently reading empty.
- Writes now invalidate the parent listing cache when the file is *closed* (not only when
  opened), so an `ls` racing a write can no longer leave a stale listing behind.

## [0.2.0] - 2026-07-01

### Added

- Context-manager support on `DucktapeFileSystem`: `with fsspec.filesystem("irods", ...) as fs:`
  releases the iRODS session and connection pool on exit.
- CI integration job running the `tests/integration` suite against a dockerized
  single-provider iRODS zone (`tests/integration/docker-compose.yml`).

### Changed

- Project status: proof-of-concept disclaimer removed; releases are now tagged and
  tracked in this changelog.

## [0.1.0] - 2026-06-29

### Added

- Initial release: fsspec filesystem backend for iRODS (`irods://`) built on
  python-irodsclient — listing, recursive listing, range reads, streaming and parallel
  reads/writes, typed errors, HIERARCHY_ERROR retries, and thread-safe session sharing.
