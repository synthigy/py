# Synthigy Python SDK

Thin, **zero-dependency** (stdlib-only) client for Synthigy's `/data`
endpoint. **ONE engine, async-native**: everything runs on an asyncio core
(`AsyncClient`) — hand-rolled keep-alive HTTP/1.1 pool, one multiplexed
SSE connection, watches as suspended coroutines. The blocking `Client`
(and every module-level verb) is a thin facade over that engine driving a
single background event-loop thread — same API scripts and notebooks
always had, ~1 extra thread total no matter how many watches are open.
Python ≥ 3.10.

## Install

```bash
pip install synthigy        # or: uv add synthigy
```

No runtime dependencies to resolve. The wheel also installs the
`synthigy-gen` codegen CLI (equivalently `python3 -m synthigy.codegen`).

## Hello world

```python
import synthigy
from synthigy import eq, gt, rel

synthigy.connect("https://synthigy.example.com",
                 client_id="my-service",
                 client_secret=os.environ["SYNTHIGY_SECRET"])

users = synthigy.search("User",
    {"active": eq(True), "age": gt(18), "_limit": 10,
     "_order_by": [["name", "asc"]]},
    {"name": None, "email": None, "roles": {"name": None}},
    acting_as=user_xid)
```

## One process, one client

`connect()` installs a module-wide default — any previous default is
destroyed first (watches close, SSE drops). All module-level verbs operate
on it. **Identity is multiplexed per-call via `acting_as=`, never a second
connect.** Constructing `synthigy.Client(...)` directly is the escape hatch
for tests.

## Async-first (BFFs, FastAPI/uvicorn services)

The async surface IS the engine — no thread hops, native cancellation:

```python
synthigy.aconnect(endpoint, client_id=..., client_secret=SECRET)  # once
c = synthigy.aclient()

rows = await c.search("Order", {"status": eq("open")}, {"total": None},
                      acting_as=user.xid)
await c.stack("Order", {"xid": oxid, "note": "expedite"}, acting_as=user.xid)

@app.get("/orders/stream")
async def stream(user=Depends(current_user)):
    async def gen():
        async with c.watch_query("order", acting_as=user.xid) as w:
            yield render(w.list())
            async for ev in w:            # derived query/* events
                yield render(w.list())
    return StreamingResponse(gen(), media_type="text/event-stream")
```

Request teardown is task cancellation in asyncio; watch cleanup is
cancellation-safe by design (`async with` guarantees close, close never
awaits). An open watch costs a buffer on the shared SSE connection — not
a thread, not a second connection.

`AsyncClient`/`Client` are themselves context managers too (like
`httpx.AsyncClient`/`httpx.Client`) — `async with AsyncClient(...) as c:` /
`with Client(...) as c:` close everything on scope exit, exception or not.
`aconnect`/`connect` (the module-default singleton) stay the norm for
long-lived processes; the context-manager form is for scripts and one-off
scoped clients (tests, short jobs).

The blocking `Client` below is for
scripts/seeds/notebooks; don't call it from a coroutine (it blocks the
loop, and calling it from its own loop thread raises).

## Reads

```python
rows  = synthigy.search("Movie", {"_limit": 5}, {"title": None})
movie = synthigy.get("Movie", {"xid": "m-1"}, {"title": None})   # flat unique-key args
rows  = synthigy.sql_template(
    "SELECT COUNT(*) AS n FROM {movie} WHERE {movie.release_year} > ?", [1990])

# XSQL — string query surface (server parses/compiles; GraphQL model)
rows = synthigy.query("""
movie (release_year > ?y:int, _limit 10)
  title
  ->genres
    name
""", {"y": 1990})
```

- Operators: `eq neq gt gte lt lte in_ nin like ilike is_null is_not_null`
  and `and_ / or_ / not_` (Python-keyword renames of the JS `and/or/not/in`).
- Selections mirror the shape you want back: `None` = scalar, nested dict =
  relation. The server's join default is **LEFT** — a projected relation
  never drops its parent, and relation args filter the related rows. To
  scope parents to those HAVING the relation, be explicit:
  `rel({...}, args={"_join": "inner"})`. The SDK injects nothing.
- **Empty relations are omitted** by the wire, never `[]` — use
  `row.get("genres", [])`.
- Counting/aggregation has **no dedicated op**: use `sql_template` or XSQL
  `_count`/`_agg` selections.

## Writes

```python
synthigy.sync("Movie", {"xid": "m-1", "title": "Dune",
                        "genres": [{"xid": "g-scifi"}]})   # upsert, REPLACES link-sets
synthigy.stack("user_rating", {"value": 5, "movie": {"xid": "m-1"}})  # additive
synthigy.slice("Movie", {"xid": "m-1"}, {"genres": [{"xid": "g-scifi"}]})  # unlink
synthigy.delete("Movie", {"xid": "m-1"})                   # soft delete
synthigy.purge("user_rating", {"value": {"_lt": 2}})        # hard delete by filter
```

**Writes are silent by default** — `sync`/`stack` answer `{"count": n}`, not
the record. Mint the id up front when you need it; that is cheaper than the
echo and makes a retried write idempotent rather than duplicating a row:

```python
from synthigy import new_xid

xid = new_xid()                                     # 22-char Base58
synthigy.sync("Movie", {"xid": xid, "title": "Dune"})          # -> {"count": 1}
synthigy.sync("Movie", {"xid": xid, "title": "Dune"},
              returning=True)                       # -> the written record
```

Batch heterogeneous ops in one round trip:

```python
from synthigy import ops
results = synthigy.exec_([
    ops.slice("User", {"xid": u}, {"roles": [{"xid": old}]}),
    ops.stack("User", {"xid": u, "roles": [{"xid": new}]}),
], acting_as=user_xid)
```

## Live data

The blessed pattern is **notify-then-refetch**: events are pokes; the SDK
re-runs the query through the IAM-filtered read path, so RLS is enforced on
every refetch.

```python
w = synthigy.watch_query("Order", {"status": eq("open")}, {"total": None},
                         acting_as=user_xid)
w.ready()
for ev in w.events():        # blocking iterator; break to stop
    if ev["type"] in ("query/added", "query/changed", "query/removed"):
        render(w.list())

w2 = synthigy.watch_sql_template(
    "SELECT COUNT(*) AS n FROM {task}", entities=["task"])
w2.ready(); print(w2.first())
```

Lower-level: `watch(interest)` (records/entities/relations, shaped
record/relation deltas with computed `changed`), `watch_schema()`,
`listen()` (raw SSE envelopes), `observe(descriptor, backfill=True)`
(one-call subscribe + reconnect + `/history` gap replay). One SSE
connection per client — all watches fuse onto it. `keep_alive=True` on
connect pins the SSE open across watch churn (BFFs).

Raw subscription calls (`subscribe`/`set_subscriptions`/...) exist but the
server set is per-identity **full-replace** — raw calls clobber a live
watch multiplexer's union. Prefer the watch family.

## History

```python
h = synthigy.history()
h.events(record_xid=xid)                  # recent events up to now
h.get_at(xid, "2026-01-01T00:00:00Z")
h.diff(xid, t1, t2)
```

Raises `HISTORY_UNAVAILABLE` when the server has no audit provider.

## Errors

Everything raises `synthigy.SynthigyError` with stable `.code`, derived
`.category` (`auth | iam | validation | not_found | conflict | rate_limit |
network | internal`) and `.retryable`. Structured fields when the server
sends them: `.hint`, `.entity`, `.path`, `.line`/`.col`, `.diagnostics`,
`.request_id` (matches the `X-Request-Id` the SDK sends — correlate with
server logs). Discriminate on `.code`, never the message.

## Auth

- Client credentials (`client_id`+`client_secret`): tokens minted from
  `/oauth/token`, cached per audience, refreshed 30s before expiry,
  single-flight; one automatic clear-and-retry on 401.
- `audience=` (or `$SYNTHIGY_AUDIENCE`): binds one audience to every mint this
  client makes. The platform's audience model is **opt-in by design** — a token
  minted naming no audience resolves to an identity-only audience that `/data`
  rejects, so without this every data call 401s. Set it to the server's `/data`
  audience, published at `/.well-known/synthigy` as `auth.oidc.audience`. Left
  unset the SDK names no audience, so an unentitled client keeps a soft 401
  rather than a hard `invalid_target`. `client.token(audience)` still overrides
  per call, for minting tokens aimed at a *different* audience.
- Static `token="..."` for scripts/tests (`token=""` for authless dev).
- With none of the above, resolution continues: under
  `SYNTHIGY_SUPERVISED=1` the SDK asks its supervising parent
  (`synthigy exec`/`agent`, or a robotics commander) for a token over the
  process's own stdio, then falls back to the `SYNTHIGY_TOKEN` env var,
  then raises `SynthigyError(code="NO_TOKEN")` with a message that teaches
  the fix. The pipe beats the env var deliberately: `exec` injects the
  cached token *and* supervises, and only the pipe can refresh mid-run.
  A bot written as `synthigy.Client(endpoint)` — nothing else — therefore
  runs unchanged bare, under `exec`, and under a production commander.
  See `docs/plans/PLAN-EXEC-IDENTITY.md`.
- `acting_as` is server-verified impersonation for **trusted confidential**
  clients (the BFF model) — the SDK never handles end-user OAuth redirects.

## Codegen

`python3 -m synthigy.codegen` turns an `.xsql` operations document + the
server schema into one typed Python module (TypedDict rows/params/inputs +
functions over the module-level verbs). The server owns the XSQL grammar —
`op:"describe"` compiles the source and returns a language-neutral IR; the
emitter renders Python from IR JSON and parses nothing.

```bash
# 1. Pull the IAM-filtered schema (commit it)
SYNTHIGY_CLIENT_ID=... SYNTHIGY_CLIENT_SECRET=... \
  python3 -m synthigy.codegen pull http://localhost:7887 schema.json

# 2. Describe + generate (saves movies.ir.json beside the .xsql — commit
#    schema.json + .xsql + .ir.json; after that gen runs OFFLINE forever)
python3 -m synthigy.codegen gen movies.xsql          # → movies_gen.py

# 3. CI drift gate: offline sourceHash check ("edited .xsql, forgot
#    codegen"); live describe-diff when SYNTHIGY_* creds are present
python3 -m synthigy.codegen check movies.xsql
```

```python
import synthigy, movies_gen as ops
synthigy.connect(endpoint, client_id=..., client_secret=...)

rows  = ops.Movie.list({"since": 2000, "limit": 10})   # list[MovieListRow]
movie = ops.Movie.detail({"xid": xid})                 # MovieDetailRow | None
w     = ops.Dashboard.watch_stats(); w.ready()         # @watch variant
both  = ops.overview()                                 # @batch → ONE round trip
ops.sync_movie({"title": "Dune", "genres": [{"xid": "g-scifi"}]})
```

Codegen authenticates **as the app** — the same client credentials the app
uses at runtime. `/schema` and `describe` are IAM-filtered per principal, so
the generated contract is exactly what the app can do; a personal/dev
identity would generate a surface the app can't honor. `--no-writes` skips
the schema-derived `<Entity>Input`/`sync_*`/`stack_*`/`delete_*` tier.
Empty or unknown op kinds in a saved IR are a hard error (a stale IR is
never silently emitted from), and data keys stay snake_case verbatim —
Python native is server native, no casing transform exists.

An end-to-end example lives in `codegen-example/`. It ships only
`movies.xsql`; you run `pull` and `gen` to produce `schema.json`,
`movies.ir.json` and `movies_gen.py` yourself. See its README.

## Tests

```bash
python3 -m unittest discover -s tests            # hermetic (stub server)
SYNTHIGY_TEST_CLIENT_ID=... SYNTHIGY_TEST_CLIENT_SECRET=... \
  python3 -m unittest tests.test_integration -v  # live (default localhost:7887)
```

Register a dedicated OAuth client for the live suite (trusted confidential,
`client_credentials`) — never share identity with a live app.

## License

MIT — see [LICENSE](LICENSE). The SDKs are permissive client libraries; the
Synthigy engine is fair-code under the Sustainable Use License.
