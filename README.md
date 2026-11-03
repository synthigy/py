# Synthigy Python SDK

Thin, **zero-dependency** (stdlib-only) client for Synthigy's `/data`
endpoint. **ONE engine, async-native**: everything runs on an asyncio core
(`AsyncClient`) — hand-rolled keep-alive HTTP/1.1 pool, one multiplexed
SSE connection, watches as suspended coroutines. The blocking `Client`
(and every module-level verb) is a thin facade over that engine driving a
single background event-loop thread — same API scripts and notebooks
always had, ~1 extra thread total no matter how many watches are open.
Python ≥ 3.10.

## Quick start

**1. Get a server and connect as your app** — steps 1–3 of
[Code generation](#code-generation).

**2. Install:**

```bash
pip install synthigy            # or: uv add synthigy — zero dependencies
```

**3. Query:**

```python
import synthigy

synthigy.connect()          # endpoint + identity from `synthigy exec`

print(sorted(synthigy.schema()["entities"]))     # what this app can see

# XSQL: the shape you write is the shape you get back
for ds in synthigy.query("""
dataset
  name
  ->versions
    name
"""):
    print(ds["name"], [v["name"] for v in ds.get("versions", [])])
```

```bash
synthigy exec -- python app.py
```

That query runs on any instance — `dataset` is the built-in model registry.

**4. Bring your own model.** Deploy a model export (the JSON the modeler
writes) and every entity in it becomes a table and an API at once:

```python
with open("My Model@1.0.json") as f:
    synthigy.deploy(f.read())                # needs the Dataset Developer role

synthigy.stack("movie", {"title": "Dune", "release_year": 2021})
rows = synthigy.query("movie (release_year >= ?y:int)\n  title\n", {"y": 2021})
```

**5. Get types** — see [Code generation](#code-generation).

Where to next: [Async-first](#async-first-bffs-fastapiuvicorn-services) for
FastAPI services, [Logging users in](#logging-users-in-clientlogin) for a
browser login, [Live data](#live-data) for watches.

## One process, one client

`connect()` installs a module-wide default — any previous default is
destroyed first (watches close, SSE drops). All module-level verbs operate
on it. **Identity is multiplexed per-call via `acting_as=`, never a second
connect.** Constructing `synthigy.Client(...)` directly is the escape hatch
for tests.

## Async-first (BFFs, FastAPI/uvicorn services)

The async surface IS the engine — no thread hops, native cancellation:

```python
synthigy.aconnect()          # once, at startup
c = synthigy.aclient()

rows = await c.query("""
movie (release_year > ?y:int, limit 10)
  title
""", {"y": 1990}, acting_as=user.xid)
await c.stack("movie", {"xid": xid, "title": "Dune"}, acting_as=user.xid)

@app.get("/movies/stream")
async def stream(user=Depends(current_user)):
    async def gen():
        async with c.watch_query_xsql("movie (limit 10)\n  title\n", entity="movie",
                                      acting_as=user.xid) as w:
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
rows  = synthigy.query("""
movie (release_year > ?y:int, limit 10)
  title
  ->genres
    name
""", {"y": 1990})
movie = synthigy.query("""
movie (xid = ?xid:string)
  title
""", {"xid": xid}, op="get")                 # one record, or None
count = synthigy.sql_template(
    "SELECT COUNT(*) AS n FROM {movie} WHERE {movie.release_year} > ?", [1990])
```

- `?name:type=default` are named params, passed as a dict.
- `->genres` is a **left** pull — a movie with no genres is still returned;
  `-genres` is **inner** and keeps only movies that have one; `!genres` keeps
  only movies with none (filters allowed, no fields under it).
- **Empty relations are omitted**, never `[]` — use `row.get("genres", [])`.
- Counting and aggregation compute in the database: `_count` / `_agg` in XSQL,
  or `sql_template`.

## Writes

```python
synthigy.sync("movie", {"xid": xid, "title": "Dune"})                  # upsert, REPLACES link-sets
synthigy.stack("user_rating", {"value": 5, "movie": {"xid": xid}})     # additive
synthigy.delete("movie", {"xid": xid})                                 # soft delete
```

`slice` (unlink without deleting) and `purge` (hard delete by filter) exist
too.

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
    ops.query("movie (limit 3)\n  title\n"),
    ops.stack("user_rating", {"value": 4, "movie": {"xid": xid}}),
], acting_as=user_xid)
```

## Live data

The blessed pattern is **notify-then-refetch**: events are pokes; the SDK
re-runs the query through the IAM-filtered read path, so RLS is enforced on
every refetch.

```python
w = synthigy.watch_query_xsql("movie (limit 5, order by release_year desc)\n  title\n",
                              entity="movie", acting_as=user_xid)
w.ready()
for ev in w.events():        # blocking iterator; break to stop
    if ev["type"] in ("query/added", "query/changed", "query/removed"):
        render(w.list())

w2 = synthigy.watch_sql_template(
    "SELECT COUNT(*) AS n FROM {movie}", entities=["movie"])
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

## Files

`client.files` speaks the engine's `/files` endpoints: the bytes, their
versions and folders, under the same access control as your data. Each file
is also a `File` record on `/data`. A file is a path (`"/site/img/logo.png"`),
an xid or the file; a folder is a path ending in `/` (`"/"` is ROOT), an xid or
the folder.

```python
files = synthigy.files()
files.folder("/site/img/")                      # made with every missing folder above it
with open("logo.png", "rb") as f:
    fi = files.put("/site/img/logo.png", f)     # creates, or replaces (the old bytes stay a version)
# fi["xid"], fi["size"], fi["hash"] (SHA-256, the download ETag), fi["uploaded_by"]["name"]

data = files.download("/site/img/logo.png")     # bytes; byte_range=(0, 1023) for part of it
files.put(fi, new_bytes, if_match=fi)           # FILE_CHANGED if someone was first
files.put({"name": "scan.pdf"}, data)           # a new file in no folder, every time
files.list("/site/")                            # {"files", "folders"}; None: no folder; omitted: every file
files.search("site logo")                       # paths holding the words in order, shortest first
files.move("/site/img/logo.png", "/brand/")     # keeps the name; never overwrites
files.move("/site/img/", "/assets/img/")        # a folder
vs = files.versions(fi)                         # the last ten, newest first
files.restore(fi, vs[1])
files.share("/site/", "read", users=[user_xid])
files.delete("/site/img/logo.png")              # or an empty folder: "/site/img/"
```

`put` never creates folders (a missing one is `FOLDER_NOT_FOUND`); `folder`
does. `put` takes bytes, a `str` (sent as UTF-8 text) or a binary file.
`download(if_none_match=hash)` answers `None` while the file is unchanged;
`info` answers `None` for a file you cannot see.

A browser cannot send your token (`<img src>`, a link in an email). Files in a
public folder load from a plain link, built without a request; `serve`
answers a browser's request from your server with your token, Range and 304
included:

```python
files.folder("/site/", public=True, cache=86400)
src = files.url("/site/img/logo.png")           # or an xid; inline=True opens it in the browser
status, headers, body = files.serve("/private/logo.png", request.headers)
```

A folder's own `public` and `cache` win, else the nearest folder above that
has one (`None` clears its own); the folder shows what applies as `public`
and `cache`, what is set on it under `settings`. File User may set the cache
but not public (`ATTRIBUTE_FORBIDDEN`).

One put carries up to the server's `SYNTHIGY_SERVER_MAX_BODY` (25 MB by
default). The engine's [files guide](https://github.com/synthigy/synthigy/blob/main/docs/FILES.md)
covers storage, access and retention; error codes include `FILE_NOT_FOUND`,
`FOLDER_NOT_FOUND`, `FILE_CHANGED`, `FILE_TOO_LARGE`, `VERSION_EXPIRED`,
`INVALID_PATH`, `AUDIT_UNAVAILABLE` and `FILES_UNAVAILABLE` (no files module
on the server).

## Errors

Everything raises `synthigy.SynthigyError` with stable `.code`, derived
`.category` (`auth | iam | validation | not_found | conflict | rate_limit |
network | internal`) and `.retryable`. Structured fields when the server
sends them: `.hint`, `.entity`, `.path`, `.line`/`.col`, `.diagnostics`,
`.request_id` (the `X-Request-Id` the server answered with — correlate with
server logs). Discriminate on `.code`, never the message.

`.retryable` is true for `network`, `rate_limit` and `internal` codes, and for
any error the server marks retryable. A code this SDK doesn't know yet is
category `internal` but not retryable.

## Auth

- Client credentials (`client_id`+`client_secret`): tokens minted from
  `/oauth/token`, cached per audience, refreshed 30s before expiry,
  single-flight; one automatic clear-and-retry on 401. A refused mint is
  `INVALID_CLIENT` (wrong id or secret, inactive client), `INVALID_SCOPE`, or
  `INVALID_AUDIENCE` (the client isn't linked to the API), else
  `UNAUTHORIZED`; the message carries the server's `error_description`.
- `audience=` (or `$SYNTHIGY_AUDIENCE`): leave it unset. Every mint names
  `PLATFORM_AUDIENCE` (`https://synthigy.com`), the audience `/data` and the
  other platform endpoints require; a token naming no audience would get an
  identity-only one that `/data` rejects. A client not linked to the platform
  API fails the mint with `INVALID_AUDIENCE`. `client.token(audience)` mints
  for a *different* API, per call.
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
- `acting_as` is server-verified impersonation for **trusted confidential**
  clients (the BFF model). `whoami(acting_as=xid)` shows who a call runs as —
  roles, superuser, and whether this client may act as users at all.

### Logging users in (`client.login`)

Authorization code + PKCE for a confidential server (BFF): the SDK owns the
protocol, your app owns sessions, cookies and routing. The SDK never serves a
route or issues a redirect — `start()` returns a URL, `complete()` takes the
callback's `code`/`state`; the HTTP handlers are yours. The one piece of state
the flow needs between `/login` and the callback lives in a `login_store` you
supply — anything with `put(state, login)` and one-shot `take(state)`, sync or
async (a dict, `redis.asyncio`, a DB table).

```python
client = synthigy.AsyncClient(ENDPOINT, client_id=ID, client_secret=SECRET,
                              login_store=synthigy.MemoryLoginStore())

@app.get("/login")
async def login(returnTo: str = "/"):
    started = await client.login.start(redirect_uri=CALLBACK, return_to=returnTo)
    return RedirectResponse(started["url"], 302)

@app.get("/auth/callback")
async def callback(code: str = "", state: str = "", error: str = ""):
    if error:                                 # user cancelled at the IdP
        back = await client.login.cancel(state)
        return RedirectResponse(back["return_to"] if back else "/", 302)
    done = await client.login.complete(code=code, state=state,
                                       redirect_uri=CALLBACK)
    # done["user"] = {"xid", "name", "scopes"}; done["tokens"]; done["return_to"]
    ...  # your session, your cookie
```

- `user["xid"]` is read from the id_token — no `/data` lookup. Pass it as
  `acting_as` to act on the user's behalf.
- `MemoryLoginStore` is **single process**: behind a load balancer the
  callback can land on a worker that never saw `/login`
  (`LOGIN_STATE_UNKNOWN`). Back the store with what holds your sessions.
- No store configured → `NO_LOGIN_STORE`; no `client_secret` →
  `LOGIN_REQUIRES_CONFIDENTIAL_CLIENT`. A browser or native app is a public
  client and uses its own PKCE library.
- `start(..., public_endpoint=)` when the browser reaches the IdP on a
  different URL than this process does (containers, reverse proxies).
- The code exchange never retries: an authorization code is one-shot.

## Code generation

Write your queries in `.xsql` files and get typed functions for them. The
server compiles the queries, so the types always match what it returns.

**1. Get a server.** In your project folder:

```bash
synthigy env init
synthigy up
```

The first `up` prints a `/setup` link; open it and pick a database. (No
browser? `synthigy up --bundle sqlite` skips the wizard.) Already have a server?
Skip this step.

**2. Deploy your data model** in the modeler (or from code with `synthigy.deploy()`, as a
client with the Dataset Developer role).

**3. Connect as your app.** Create its client once, then save it to the
project:

```bash
synthigy iam add-client "My App" --id my-app --type confidential \
  --role "Dataset Explorer" --api Synthigy --grant client_credentials --local
synthigy connect http://localhost:7887 --client-id my-app
```

`add-client` prints the secret once; `connect` asks for it. Code is generated
for what this app is allowed to see. (`--local` works on the server's own
machine; for a remote server, create the client in the console.)

**4. Install the SDK:**

```bash
pip install synthigy
```

**5. Write a query** in `xsql/movies.xsql`:

```
@search list
movie (release_year > ?since:int=1990, limit ?limit:int=20)
  xid
  title
  release_year
```

**6. Generate:**

```bash
synthigy exec -- python -m synthigy.codegen gen xsql/ --out ops.py
```

This writes `ops.py`, and saves `xsql/schema.json` and `xsql/ops.ir.json`
next to your queries.

**7. Use it:**

```python
import synthigy
import ops

synthigy.connect()
movies = ops.Movie.list({"since": 2000})
print([m["title"] for m in movies])
```

```bash
synthigy exec -- python app.py
```

`synthigy exec` gives your program the server address and the app's identity.
Without it, pass them yourself: `synthigy.connect(endpoint, client_id=..., client_secret=...)`.

**After you edit a query**, run step 6 again. As long as the `.xsql` files are
unchanged it works offline from `xsql/ops.ir.json`; after an edit it needs the
server, and it never generates from outdated results. In CI:

```bash
python -m synthigy.codegen check xsql/
```

**What to commit:** your `.xsql` files and `xsql/ops.ir.json`.
`xsql/schema.json` is your whole data model, so commit it only in a private
repo.

`@watch` ops also get `watch_<name>()`, `@batch` ops become one-round-trip functions, every entity gets typed `sync_<entity>` / `stack_<entity>` / `delete_<entity>`, and each namespace has an `...Async` twin for asyncio apps.

## Tests

```bash
python3 -m unittest discover -s tests            # hermetic (stub server)
SYNTHIGY_TEST_CLIENT_ID=... SYNTHIGY_TEST_CLIENT_SECRET=... \
  python3 -m unittest tests.test_integration -v  # live (default localhost:7887)
```

Register a dedicated OAuth client for the live suite (trusted confidential,
`client_credentials`) — never share identity with a live app.
`TestLiveLogin` drives the real authorization-code flow headlessly and needs
its own code-flow client plus a password user:
`SYNTHIGY_TEST_LOGIN_CLIENT_ID/_SECRET/_USER/_PASSWORD` (setup snippet in
`tests/test_integration.py`).

`tests.test_files_live` runs `client.files` and `AsyncClient.files` against an
engine with the files module, as three client-credentials clients whose service
users hold File Admin, File User and File Reader. It skips unless
`SYNTHIGY_FILES_TEST_ENDPOINT` and
`SYNTHIGY_FILES_TEST_{ADMIN,USER,READER}_{ID,SECRET}` are set:

```bash
python3 -m unittest tests.test_files_live -v
```

## License

MIT — see [LICENSE](LICENSE). The SDKs are permissive client libraries; the
Synthigy engine is source-available under the Business Source License 1.1.
