# Changelog

All notable changes to `synthigy` (PyPI). Follows [semver](https://semver.org).
Pre-1.0: breaking changes can land on minor bumps.

## 0.1.1

### Added
- **`deploy(export_contents)` and `destroy(dataset_xid)`** — on `AsyncClient`,
  on the blocking facade, as `ops.deploy` / `ops.destroy` builders, and as
  module-level delegates. A model export goes over the wire verbatim as a
  string and the server decodes it, so no client needs a transit codec:
  `deploy` posts the export file's bytes, and `destroy` is `delete` on the
  `dataset` meta-entity. The deploy ack carries the dataset xid that
  `destroy` takes, so a caller holding nothing but the export can still tear
  down what it deployed. Both are scope-gated server-side (`dataset:deploy` /
  `dataset:delete`, which only the Dataset Developer role carries), so the
  SDK adds no permission surface of its own.

### Changed
- **The platform audience is now the default; nobody configures it.** A
  `client_credentials` mint naming no audience resolves to the identity-only
  OIDC audience, which `/data`, `/schema`, `/history`, `/logs` and
  subscriptions all reject — so every user had to set `SYNTHIGY_AUDIENCE` to a
  constant they could not look up, since the server does not advertise it in
  discovery. The failure was a bare 401 that said nothing about audiences.
  This SDK is the client for the platform API, so that is what it now mints
  for. Minting for a different API stays a per-call argument. The environment
  variable remains as an escape hatch.

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
