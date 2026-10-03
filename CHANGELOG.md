# Changelog

All notable changes to `synthigy` (PyPI). Follows [semver](https://semver.org).
Pre-1.0: breaking changes can land on minor bumps.

## 0.3.2

### Added
- **`whoami(acting_as=)`** on `Client`, `AsyncClient` and the module (plus
  `ops.whoami()`) — who a call runs as: xid, name, active, superuser, roles
  (group-conferred included), groups, and `client` `{id, trusted}`, where
  `trusted` means this client may act as users. Needs engine v0.2.11 or newer.

### Removed
- **`python -m synthigy.codegen pull`.** It wrote `./schema.json` in the
  working directory while `gen` reads `schema.json` beside the `.xsql` files.
  `gen --pull` refreshes the file `gen` reads; `synthigy schema pull` writes
  the same `xsql/schema.json`.

## 0.3.1

### Fixed
- **A login may take 10 minutes, not 5** (`MemoryLoginStore()`'s default,
  `synthigy.login.LOGIN_TTL_SECONDS`). Synthigy's login page allows 8, so a
  user who took 5–8 minutes logged in there and then failed the callback with
  `LOGIN_STATE_UNKNOWN`.
- **`login.complete()` and `login.cancel()` refuse a login older than that
  themselves.** Expiry used to live only in the store, so a custom store that
  never expired accepted a login of any age.
- **Permission denials are `iam`, not retryable `internal`.**
  `RELATION_FORBIDDEN`, `ATTRIBUTE_FORBIDDEN`, `ROW_FORBIDDEN`,
  `CREATE_FORBIDDEN`, `DELETE_FORBIDDEN` and `SLOT_OCCUPIED` were missing from
  the code table, so a denied relation read came back as a retryable server
  error.
- **A code the SDK doesn't know is no longer retryable.** It stays category
  `internal`, but `.retryable` is true only when the server marks the error
  retryable (it does for transient database failures) or the code is a known
  `network`/`rate_limit`/`internal` one. `SynthigyError(..., retryable=)`
  takes the server's flag.
- **A refused token mint says why.** `invalid_client` → `INVALID_CLIENT` (with
  a `.hint` to check the client id and secret), `invalid_scope` →
  `INVALID_SCOPE`, `invalid_target` → `INVALID_AUDIENCE`; other refusals stay
  `UNAUTHORIZED`. A wrong secret used to read the same as an expired session.
- `INVALID_CONTEXT` (the blocking `Client` called from its own event-loop
  thread) is category `validation`, no longer retryable `internal`.

## 0.3.0

### Added
- **`connect()` / `aconnect()` / `Client()` / `AsyncClient()` take the
  endpoint from `SYNTHIGY_ENDPOINT` when none is passed.** Under
  `synthigy exec` that and the identity are already set, so `synthigy.connect()`
  with no arguments is the whole setup. No endpoint anywhere raises
  `NO_ENDPOINT` (category `validation`, not retryable).
- **Codegen takes several `.xsql` files or directories.** `gen` and `check`
  accept any mix of files and directories (searched recursively, sorted by
  path); one module is generated for all of them. The default output is
  `<name>_gen.py` for a single file and `ops_gen.py` otherwise.

### Changed
- **Codegen no longer falls back to `http://localhost:7887`.** With no
  endpoint passed and no `SYNTHIGY_ENDPOINT`, a run that needs the server
  fails with `NO_ENDPOINT`, as the client does; offline runs from a current
  `ops.ir.json` are unaffected.
- **Codegen reads and writes `ops.ir.json`** in the `.xsql` directory, the IR
  file every other SDK's generator shares (was `<file>.ir.json` beside each
  source). A CI `check` gate pointing at the old file must be re-pointed;
  the first `gen` writes the new one.
- **Codegen:** each `.xsql` file is sent to `describe` separately (`sources: [{path,
  source}]`) instead of one concatenated text. Concatenation leaked the
  first file's buffer-level `@namespace` into every later file and dropped
  the others'. Needs an engine with per-file `describe`. A `(namespace,
  name)` declared twice across files now fails with `DUPLICATE_OPERATION`,
  naming both files; a `@batch` naming an op from another file fails with
  `BATCH_MEMBER_UNRESOLVED`. Both are `validation` in the error table.
  A validation failure from `describe` no longer adds the misleading
  "run under `synthigy exec`" hint.

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
