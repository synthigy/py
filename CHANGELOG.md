# Changelog

All notable changes to `synthigy` (PyPI). Follows [semver](https://semver.org).
Pre-1.0: breaking changes can land on minor bumps.

## 0.6.0

### Breaking
- **`observe()` is gone; use `watch()`.** It opened its own event stream and posted its own subscription
  set, and the engine keeps one set per client, so it overwrote the set every watch shared. For the same
  loop: `for ev in client.watch({"records": records}).events()`. Its `backfill` lives on as a `watch`
  option.
- **`subscribe`, `unsubscribe`, `subscribe_model`, `unsubscribe_model`, `set_subscriptions` and
  `clear_subscriptions` are internal** (on `Client`, `AsyncClient` and the module). Each replaced the
  client's whole subscription set behind the watch multiplexer's back. Declare interest with `watch()`;
  `subscriptions()` still shows what the engine holds.

### Added
- **Each event stream has its own subscription set** (engine 0.5.5+). Every connection gets a fresh stream id
  (`/data/events?stream=…`, also on `listen()`'s `sse/open` as `"stream"`) and each subscription POST names it,
  so two processes on one client id no longer overwrite each other's watches. An older engine ignores the id
  and keeps one set per client.
- **`watch(interest, backfill=True)`** replays the record events missed during a reconnect from `/history`
  (asked per watched record, `backfill_limit=1000` rows each, marked `fromBackfill`), then emits
  `connection/resumed`.
- **A busy engine is retried for you.** `SERVER_BUSY` (HTTP 503 from an overloaded engine) is resent up to
  3 attempts in all, waiting `Retry-After` (at most 10 s) with ±50% jitter, never past the client's `timeout`; a per-call `timeout=` covers the waits. The request never
  ran, so this is safe for writes too. When all 3 are refused you get the error: category `rate_limit`, retryable.

### Fixed
- **`events()` opened after a refused stream gets the refusal and ends.** It used to get a fresh buffer that never
  received the `subscription/rejected` and never closed, so the loop hung.
- A subscription POST that returns after its event stream ended no longer marks the watches it covered
  subscribed; the engine dropped that stream's set, so they settle on the next open's POST.
- **Live queries re-run on `connection/resumed`**, so a dropped connection no longer leaves a `watch_query` or
  `watch_sql_template` stale until the next change.
- A backfilling watch holds the live events that arrive while it replays the gap and delivers them after the
  replayed ones and `connection/resumed` (also when the replay fails), so an older replayed event can't land
  after a newer live one.
- A subscription POST answered `STREAM_NOT_FOUND` (its stream already closed) is no longer reported as
  `subscription/rejected`; the next stream posts the union.
- A watch no longer reports itself ready while no stream is open: an unchanged union during a reconnect or
  after `STREAM_NOT_FOUND` used to mark it subscribed although nothing was posted.
- A watch opened after a refused stream (401/403) opens a new stream; it used to wait on the dead one forever.
- A refused stream's `subscription/rejected` reaches a watch that is replaying a backfill gap; it used to end
  with no event.
- Watches ended by a refused stream leave the shared subscription, so the next stream posts only the interest
  of watches still open; replacing a refused stream no longer logs "Task exception was never retrieved".
- **A busy resend stays within the client's `timeout`.** A `SERVER_BUSY` resend now gets only what is left of the
  client-level `timeout`, not a fresh full one, so the whole call ends within about that timeout.
- **A watch on every operation is no longer narrowed by another watch's op filter.** The shared subscription
  sent the union of every watch's ops, so one watch filtered to `insert` stopped updates and deletes for watches
  that wanted all of them. Any watch with records and no op filter now turns the filter off.
- **45 engine error codes now carry their real category** instead of `internal`: bad shapes, `INVALID_*` and
  missing arguments are `validation`; `REF_NOT_FOUND`, `UNKNOWN_ATTRIBUTE`, `UNKNOWN_ROLE` and `DOC_NOT_FOUND` are
  `not_found`; `GUARD_VIOLATION`, `CONSTRAINT_VIOLATION`, `DEPENDENT_OBJECTS`, `HOOK_REFUSED`, `LOCKED` and
  `SCHEMA_CONFLICT` are `conflict`; `DB_UNAVAILABLE` is `internal` and retryable. `DB_ERROR` stays unclassified
  and not retryable; the engine's own `retryable` flag still wins.

## 0.5.0

### Added
- **Errors keep `rule`, `attribute` and `doc`.** `.rule` (the rule that refused),
  `.attribute` and `.doc` (its manual section, read at `GET /docs/<doc>`) when the
  server sends them, at the top level and per operation. Per-operation `compile`
  errors, `history`, subscription and generated `@batch` member errors keep the
  same structured fields as every other error (`hint`, `details`, `retryable`,
  `rule`, `attribute`, `doc`, …), not only the message and code; regenerate for
  the `@batch` change. `history` and subscription errors carry `.status` too.
- **`"error"` on `subscription/rejected`**: the `SynthigyError` that refused the
  subscribe or the watch's first query (`code`, `hint`, `rule`, `doc`, …), beside
  the unchanged `"reason"`.

### Fixed
- `destroy` returns `True` once the operation succeeds, also when the engine
  answers `null`.
- `schema()`, `lint()` and `compile()` answered every non-2xx with `HTTP_ERROR`;
  when the reply carries the server's error, they raise it, its code and fields
  included.
- `onboard` / `onboard_complete` raised `AttributeError` on an engine error
  object (a router 404, a 500, a 503 `MODULE_NOT_LOADED`); they raise the
  engine's error, its code and fields included. Bare-string codes are unchanged.
- A generated module without `@batch` raised `NameError` instead of
  `PARAM_UNKNOWN` for an unknown param; regenerate.
- The `NO_TOKEN` message offered `SYNTHIGY_CLIENT_ID` + `SYNTHIGY_CLIENT_SECRET`,
  which the client never reads from the environment. It names what works:
  `SYNTHIGY_TOKEN`, `synthigy exec`, or `client_id` and `client_secret` passed to
  `connect()`.
- SSE connect (`listen`, `observe`, watches) raises the engine's error from the response
  body, its hint, rule and doc included. A 403 ends the stream whatever its code.
- Code generator describe failures show the engine's position, hint and manual section
  (`docs: <endpoint>/docs/<doc>`), not only the code and message.

## 0.4.0

Needs engine v0.5.0 or later.

### Breaking
- **No client-wide `acting_as`.** `Client(acting_as=…)` / `AsyncClient(acting_as=…)`
  is a TypeError: the client runs as itself; pass `acting_as=` per call.

### Added
- **Acting as a user on files.** `acting_as=` on every `files` call (sync and async); sent as `X-Acting-As`.
  A trusted BFF works on files as the user, like `/data`.

### Fixed
- A write right after `watch_query` / `watch_query_xsql` / `watch_sql_template`
  became ready could be missed: `ready()` returned once the stream opened, before
  the engine held the subscription. It now waits for that and refetches once.
## 0.3.4

Needs an engine with `POST /files/zip` (after v0.4.1).

### Breaking
- **`_count` / `_agg` are gone** (engine after v0.4.1 refuses them).
  Write an operation per value: XSQL `votes: count(ratings)`,
  `rating: avg(ratings.value)`; on the wire an `{alias, op, args?}` entry
  on the relation key. The alias is the result key and can be sorted by.
- `python3 -m synthigy.codegen` types an operation as a plain field (counts `int`, never null;
  `avg` float; `sum`/`min`/`max` the field's type). The `_count` / `_agg`
  map fields are gone; regenerate.

### Added
- **`files.zip(files=[...], folders=[...])`**: one zip (bytes) of the files
  and of everything inside the folders, read as you. `ZIP_TOO_LARGE` joins the
  error codes.
- **`files.link(file, *, expires=None, max_downloads=None, inline=None)`**,
  `files.links(file)`, `files.unlink(link | xid)`: download links for people
  without an account, where the folder's `can_link` allows them;
  `files.folder(path, can_link=...)`. `LINKS_DISABLED`, `LINK_NOT_FOUND` and
  `INVALID_EXPIRES` join the error codes.
- `files` answers follow the client's `key_format`, as `/data` results do
  (needs an engine that reads `key_format` on `/files`).

### Fixed
- A configured `timeout` no longer cuts file bytes short: `files.put`,
  `download`, `zip` and `serve` run as long as the transfer takes.
- `FILES_MIGRATING` (uploads paused while the server moves files to a new store) is now category `internal` and retryable; it was unknown, so never retryable.

## 0.3.3

Needs engine v0.4.1 or newer for files.

### Added
- **`client.files`** (and `synthigy.files()`, `AsyncClient.files`): `put` (a
  path creates or replaces the file there, its folder must exist; an xid or
  file replaces it; `{"name": ...}` makes a file in no folder; `if_match`),
  `download` (`byte_range`, `if_none_match`, `version`), `info`, `list` (a
  folder's `{"files", "folders"}`; `None`: files in no folder; omitted: every
  file you can see; `q` (words in order, a space for anything between),
  `limit`, `offset`), `search` (folders and files whose paths hold the words in
  order, shortest path first), `delete` (a file, or an empty folder), `url` (a
  link without a request, `inline`), `move` (a file or a folder), `folder` (made
  with every missing folder above it; `public`, `cache`), `share`, `unshare`,
  `access`, `versions`, `restore` and `serve` (`(status, headers, body)` that
  passes Range and 304 through). A file is a path, an xid or the file; a
  folder a path ending in `/`, an xid or the folder. New error codes:
  `FILE_NOT_FOUND`, `FOLDER_NOT_FOUND`, `FILES_UNAVAILABLE`,
  `AUDIT_UNAVAILABLE`, `VERSION_EXPIRED`, `NOT_FOUND` → `not_found`;
  `FILE_NAME_TAKEN`, `FILE_CHANGED`, `FILE_NOT_READY`, `FOLDER_NAME_TAKEN`,
  `FOLDER_NOT_EMPTY`, `FOLDER_CYCLE` → `conflict`; `NAME_REQUIRED`,
  `INVALID_NAME`, `INVALID_ACCESS`, `INVALID_PATH`, `INVALID_SETTINGS`,
  `INVALID_QUERY`, `METHOD_NOT_ALLOWED`, `INVALID_SLICE`, `FILE_TOO_LARGE`,
  `UNSUPPORTED_MEDIA_TYPE`, `RANGE_NOT_SATISFIABLE` → `validation`. Needs a
  server with the files module and File System 0.3.10.

### Fixed
- **A 204 or 304 answer no longer hangs the call.** The transport read a response
  without `Content-Length` until the connection closed, which a keep-alive server
  never does. Bodiless statuses (1xx, 204, 304) now end at their headers.

### Changed
- Error categories for the deploy checks: `SYSTEM_DATASET_ENGINE_OWNED` and
  `SYSTEM_ENTITY_ALTERED` → `iam` (a superuser may); `ENTITY_NAME_CONFLICT`,
  `FIELD_KEY_CONFLICT` and `IMMUTABLE_VERSION_ALTERED` → `validation`. They used to
  fall back to `internal`.
- `UNKNOWN_TREE_RELATION` (the engine refuses an `on` of a tree op that is not a tree relation of
  the entity) → `validation`.
- More codes categorized: `ENGINE_OWNED` (a meta-model row or field only deploy and
  recall write) → `iam`; `SYSTEM_ENTITY_RLS`, `RLS_GUARD_BROKEN` and
  `VERSION_NAME_TAKEN` → `validation`; `MODEL_BUSY` (another deploy, recall or destroy
  is running; retry when it finishes) → `conflict`.

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
