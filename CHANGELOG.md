# Changelog

All notable changes to `synthigy` (PyPI). Follows [semver](https://semver.org).
Pre-1.0: breaking changes can land on minor bumps.

## 0.1.1

### Fixed
- **`gen --out` into a directory that did not exist raised `FileNotFoundError`.**
  The output directory is normally gitignored, so the first generation after a
  fresh clone hit it. The generator now creates it.

## 0.1.0

First public release.

### Added
- Async-native `/data` client: `AsyncClient` over a hand-rolled keep-alive
  HTTP/1.1 pool, one multiplexed SSE connection, watches as suspended
  coroutines. Blocking `Client` and module-level verbs are a facade over the
  same engine on one background event-loop thread.
- Full CRUD verb set, XSQL queries, SQL templates, schema introspection,
  the temporal `/history` API, record subscriptions and the watch family.
- OAuth client-credentials and supervised token sources.
- `synthigy-gen` — typed Python codegen from an `.xsql` operations document
  plus the IAM-filtered schema, with an offline drift gate (`check`).
- `py.typed`: annotations are visible to downstream type-checkers.

### Fixed
- A watch closed while the previous subscription flush was still in flight
  could leave the server's union stale — coalescing skipped the tail change.
  A flush now re-runs when interest moved under it.

Zero runtime dependencies — standard library only. Python ≥ 3.10.
