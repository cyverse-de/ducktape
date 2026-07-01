# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Before 1.0.0, minor-version bumps may include breaking API changes.

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
