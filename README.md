# ducktape

An [fsspec](https://filesystem-spec.readthedocs.io/) filesystem backend for
[iRODS](https://irods.org/), built on
[python-irodsclient](https://github.com/irods/python-irodsclient).

> **Status: proof of concept — not production-ready.** ducktape is an early prototype
> under active development. It has been exercised against a live iRODS deployment but has
> not been hardened, performance-tuned, or stabilized for production use; APIs and
> behavior may change without notice. Use it for evaluation and experimentation only.

It exposes iRODS under the `irods://` protocol so tools that speak fsspec can read and
write iRODS data objects. The primary consumers are DuckDB (via `register_filesystem`)
and a web-based data manager. It is designed for large directory listings (10k+ entries)
and large file transfers (10s of GB).

Supported operations: listing (`ls`/`info`/`exists`), recursive listing (`find`/`walk`/
`glob`, which fetch a whole subtree in a constant number of catalog queries), reading (range
reads + whole-file parallel `get`), writing (streaming `open("wb")` + whole-file parallel
`put`, both driving fsspec progress callbacks), `mkdir`/`makedirs`/`rmdir`, idempotent `rm`,
and server-side `copy`. Append mode is not supported.

## Paths

iRODS logical paths are absolute and rooted at the zone, with **no host in the path**
(the host is connection configuration). The canonical URL form therefore uses an empty
authority — three slashes:

```
irods:///tempZone/home/rods/data.parquet   ->   /tempZone/home/rods/data.parquet
```

`irods://host/...` is rejected: the host belongs in `storage_options`.

## Usage

```python
import fsspec

# Explicit credentials (per-user / multi-tenant use):
fs = fsspec.filesystem(
    "irods",
    host="irods.example.org", port=1247,
    user="rods", password="...",   # password may also come from IRODS_PASSWORD
    zone="tempZone",
)

# Or rely on the standard iRODS environment file (service-account use):
#   fs = fsspec.filesystem("irods")   # uses ~/.irods/irods_environment.json (.irodsA/PAM)
```

### With DuckDB

```python
import duckdb, fsspec

con = duckdb.connect()
con.register_filesystem(fsspec.filesystem("irods", host="...", user="...",
                                          password="...", zone="tempZone"))
con.sql("SELECT count(*) FROM read_parquet('irods:///tempZone/home/rods/data.parquet')")
con.sql("COPY (SELECT 1 AS x) TO 'irods:///tempZone/home/rods/out.parquet' (FORMAT PARQUET)")
```

### Concurrency note

Data objects are opened with `allow_redirect=False` by default. On iRODS ≥ 4.3.1 the
redirect path clones the session to the replica's resource host per open, which races
under concurrent opens (`HIERARCHY_ERROR`) and undermines connection pooling. Pass
`allow_redirect=True` only for single-stream access where direct-to-resource routing is
worth that cost.

Even with redirect off, concurrent opens can still intermittently hit `HIERARCHY_ERROR`
while the server resolves the resource hierarchy. Opens (and parallel `get`/`put`) are
therefore retried automatically — `hierarchy_retries` times (default `3`) with a
`hierarchy_retry_backoff`-second linear backoff (default `0.1`). Set `hierarchy_retries=0`
to disable. Range reads on an already-open file are serialized by a per-file lock, so a
single file object is safe to read from multiple threads (as DuckDB does).

## Authentication

Resolved by `ducktape.auth.resolve_auth`, precedence **explicit > environment file**:

- **Explicit** — pass `host`/`port`/`user`/`password`/`zone` as `storage_options`. The
  password falls back to the `IRODS_PASSWORD` env var so it need not appear as a visible
  option.
- **Environment file** — otherwise the standard iRODS env file is used, located via
  `irods_env_file` option → `IRODS_ENVIRONMENT_FILE` env → `~/.irods/irods_environment.json`.

### TLS and other connection options

Explicit mode forwards anything in `connection_options` straight to `iRODSSession`, so you
can require TLS (or set any other PRC connection setting) against servers that need it:

```python
fs = fsspec.filesystem(
    "irods",
    host="irods.example.org", user="rods", password="...", zone="tempZone",
    connection_options={
        "client_server_negotiation": "request_server_negotiation",
        "client_server_policy": "CS_NEG_REQUIRE",   # require encryption
        "ssl_verify_server": "cert",
    },
)
```

Environment-file mode reads negotiation/TLS settings from the JSON file as usual.

## Development

```sh
uv sync
uv run pytest
uv run ruff check
uv run ruff format --check
uv run pyright
```

Integration tests under `tests/integration/` are skipped unless `IRODS_TEST_HOST` is set
(they require a reachable iRODS server).
