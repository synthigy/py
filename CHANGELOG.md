# Changelog

All notable changes to `synthigy` (PyPI). Follows [semver](https://semver.org).
Pre-1.0: breaking changes can land on minor bumps.

## 0.2.0

### Changed
- **Token failures are `SynthigyError`**: an `/oauth/token` refusal raises
  `UNAUTHORIZED` (`.status`, the RFC 6749 error in `.details`, its description
  in the message); an unreachable token endpoint raises `NETWORK_ERROR`. Was a
  bare `RuntimeError`.
- **`NO_TOKEN` is no longer retryable** (category `auth`, was `internal`).
- **Generated modules import on Python 3.10 with zero dependencies**: the
  `NotRequired` fallback no longer requires `typing_extensions` (it was an
  undeclared dependency; import failed on 3.10 without it).
- **`login.complete()` refuses an id_token with no `xid` claim**
  (`LOGIN_EXCHANGE_FAILED`) instead of returning `xid: None`, which as
  `acting_as` would have run the user's calls as the service.
- **Generated functions take `acting_as=` / `timeout=` explicitly** instead of
  `**opts`, so a param passed as a keyword (`Rack.racks(q=...)`) fails at the
  call instead of deep in the client. Regenerate to pick it up.

### Added
- **`PARAM_UNKNOWN`** — generated operations and batches reject params the
  operation doesn't declare (the server silently ignores them), e.g. a
  per-member `{"racks": {...}}` dict passed to a batch, whose params are one
  flat dict shared by every member.
- Generated `*Input` types leave out retired enum values when `/schema`
  publishes `retired` — they read back but the engine refuses to write them.
- **`client.login.start / complete / cancel`** — OIDC authorization code + PKCE
  for confidential servers (BFFs), on `AsyncClient`, the blocking facade and
  `synthigy.login()`. The SDK owns the protocol (PKCE, state, nonce, code
  exchange, id_token → `{xid, name, scopes}`); sessions stay the app's. In-flight
  logins live in a pluggable `login_store=` (`LoginStore` protocol, sync or
  async); `MemoryLoginStore` for single-process dev. New error codes:
  `NO_LOGIN_STORE`, `LOGIN_REQUIRES_CONFIDENTIAL_CLIENT`, `LOGIN_STATE_UNKNOWN`,
  `LOGIN_NONCE_MISMATCH`, `LOGIN_EXCHANGE_FAILED`. Zero new dependencies.
- **`compile(source, params=None, *, op="search")`** — on `AsyncClient`, on the
  blocking facade, and as a module-level delegate. `POST /compile` returns the
  wire operation the engine would execute for an XSQL source, without executing
  it. The compiler is the authority on the wire format, so this is how you take
  programmatic control of a query instead of hand-writing the dict: edit what
  comes back and pass it to `exec_()`. `params` bind exactly as on `query`, so
  the result IS what the engine receives.

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
