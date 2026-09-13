"""datastar-music — Synthigy Python SDK live-data showcase.

    Browser (Datastar + Tyrell, no JS framework)
       ↕ HTML + SSE patches
    FastAPI BFF (this file)
       ↕ synthigy SDK  (typed reads/watches + writes + batch + history)
    Synthigy server (/data, /oauth, /data/events, /history)

ONE async SDK client per process → ONE upstream SSE to Synthigy, however
many browser tabs are open. Everything here — page loads, writes, batch,
history, live watches — is `await`ed natively on `synthigy.aclient()`:
no thread pool, no asyncio.to_thread, no second client. An open watch is
a suspended coroutine on the client's shared SSE connection, not a parked
OS thread (the thread-per-watch design this replaced starved the pool and
hung the demo under browser-tab churn).

Each page opens one SSE browser→BFF; the BFF turns SDK watch events into
Datastar element patches. Live updates use notify-then-refetch: an event
just pokes; we re-read the RLS-scoped query (`acting_as`) and repaint the
fragment. Writes never repaint anything themselves — the watch does.
Tokens never touch the browser.

Run (from this directory, after codegen — see README):
    uvicorn serve:app --port 5175 --reload --timeout-graceful-shutdown 3
Required env: SYNTHIGY_CLIENT_ID, SYNTHIGY_CLIENT_SECRET
Optional:     SYNTHIGY_ENDPOINT (default http://localhost:7887),
              PORT (5175), BASE_URL (http://localhost:$PORT)
"""

from __future__ import annotations

import os

from datastar_py.consts import ElementPatchMode as Mode
from datastar_py.fastapi import DatastarResponse, ReadSignals
from datastar_py.fastapi import ServerSentEventGenerator as SSE
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse

import synthigy
from synthigy import SynthigyError, eq, gt

import auth
import views
from generated.music_gen import (
    DashboardAsync,
    MusicAlbumAsync,
    overview_async,
)
from seed import xid as det_xid  # deterministic xid from a natural key

ENDPOINT = os.environ.get("SYNTHIGY_ENDPOINT", "http://localhost:7887")
CLIENT_ID = os.environ.get("SYNTHIGY_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SYNTHIGY_CLIENT_SECRET")
PORT = int(os.environ.get("PORT", "5175"))
BASE_URL = os.environ.get("BASE_URL", f"http://localhost:{PORT}").rstrip("/")
REDIRECT_URI = f"{BASE_URL}/auth/callback"
PER_PAGE = views.PER_PAGE
TOP_N = 8

if not CLIENT_ID or not CLIENT_SECRET:
    raise SystemExit("SYNTHIGY_CLIENT_ID and SYNTHIGY_CLIENT_SECRET are required "
                     "(see README for how to register an OAuth client).")

# THE client — async-native, module default. Generated typed ops
# (MusicAlbumAsync, DashboardAsync, overview_async) and every ad-hoc
# aclient() verb below run on it. Writes take the same typed *Input dicts
# via the generic verbs: `await synthigy.aclient().stack(entity, data)`.
synthigy.aconnect(ENDPOINT, client_id=CLIENT_ID, client_secret=CLIENT_SECRET,
                  timeout=15)

app = FastAPI()


@app.on_event("shutdown")
async def _close_async_client():
    await synthigy.adisconnect()


def _session(request: Request) -> auth.Session | None:
    return auth.get_session(request.cookies.get(auth.SESSION_COOKIE))


def _login_redirect(return_to: str) -> RedirectResponse:
    return RedirectResponse(f"/login?returnTo={return_to}", status_code=302)


# UI sort keys → `?sort(plays, title, created_on)` spec strings. The XSQL
# op declares the default ("plays desc") and the sortable set; the server
# validates the bound value at compile time.
SORTS = {
    "plays":  "plays desc",         # popularity (default)
    "az":     "title asc",
    "za":     "title desc",
    "newest": "created_on desc",
}


def _window(q: str, limit: int, hits: bool, sort: str) -> dict:
    """The typed-op params every albums control funnels into — all five
    XSQL params of MusicAlbum.list in one place."""
    return {"q": f"%{q.strip()}%" if q.strip() else "%",
            "limit": max(1, min(limit, 200)),
            "offset": 0,
            "min_plays": views.HITS_MIN_PLAYS - 1 if hits else -1,
            "sort": SORTS.get(sort) or SORTS["plays"]}


def _toast(msg: str, flavor: str = "success", **kw):
    return SSE.patch_elements(views.toast_html(msg, flavor, **kw),
                              selector="#toast", mode=Mode.INNER)


def _err_toast(e: Exception):
    msg = e.args[0] if isinstance(e, SynthigyError) and e.args else str(e)
    return DatastarResponse([_toast(f"Failed: {msg}", "danger")])


# ── pages ────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    sess = _session(request)
    return views.landing_page(sess.user if sess else None)


@app.get("/albums", response_class=HTMLResponse)
async def albums(request: Request, q: str = "", limit: int = PER_PAGE,
                 hits: bool = False, sort: str = "plays"):
    sess = _session(request)
    if not sess:
        return _login_redirect("/albums")
    rows = await MusicAlbumAsync.list(_window(q, limit, hits, sort),
                                      acting_as=sess.user.xid)
    return views.albums_page(sess.user, rows, q.strip(), hits, sort)


async def _fetch_history(xid: str):
    """Audit timeline for one record — /history over the audit plug.
    Absent provider (404 → HISTORY_UNAVAILABLE) just hides the section."""
    try:
        r = await synthigy.aclient().history.events(record_xid=xid, limit=10)
        return r.get("events") if isinstance(r, dict) else r
    except Exception:
        return None


@app.get("/albums/{xid}", response_class=HTMLResponse)
async def album_detail(request: Request, xid: str):
    sess = _session(request)
    if not sess:
        return _login_redirect(f"/albums/{xid}")
    alb = await MusicAlbumAsync.detail({"xid": xid}, acting_as=sess.user.xid)
    if not alb:
        return HTMLResponse("<h1>Not found</h1>", status_code=404)
    events = await _fetch_history(xid)
    return views.album_detail_page(sess.user, alb, views.history_html(events))


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    sess = _session(request)
    if not sess:
        return _login_redirect("/dashboard")
    # @batch: top-N list + stats in ONE wire request …
    batch = await overview_async({"limit": TOP_N}, acting_as=sess.user.xid)
    # … plus an ad-hoc data-shaped search with a filter helper.
    bclub = await synthigy.aclient().search(
        "music_album",
        {"plays": gt(1_000_000_000), "_order_by": {"plays": "desc"},
         "_limit": 5},
        {"xid": None, "title": None, "plays": None, "cover": None},
        acting_as=sess.user.xid)
    stats_rows = batch["stats"]
    stats = stats_rows[0] if isinstance(stats_rows, list) and stats_rows else {}
    top = batch["list"] if isinstance(batch["list"], list) else []
    return views.dashboard_page(sess.user, stats, top, bclub)


# ── debounced live search / window growth (one-shot SSE) ─────────────────

@app.get("/albums-search")
async def albums_search(request: Request, q: str = "", limit: int = PER_PAGE,
                        hits: bool = False, sort: str = "plays"):
    sess = _session(request)
    if not sess:
        return PlainTextResponse("", status_code=401)
    term = q.strip()
    win = _window(q, limit, hits, sort)
    rows = await MusicAlbumAsync.list(win, acting_as=sess.user.xid)
    return DatastarResponse([
        SSE.patch_elements(views.albums_list_html(rows, term),
                           selector="#albums", mode=Mode.INNER),
        SSE.patch_elements(views.search_meta(term, len(rows)),
                           selector="#albums-meta", mode=Mode.INNER),
    ])


# ── writes ───────────────────────────────────────────────────────────────
# None of these repaint data. They write, toast, and let the watches do the
# rest — the same event stream every open tab is already on.

@app.post("/albums/new")
async def create_album(request: Request, signals: ReadSignals):
    sess = _session(request)
    if not sess:
        return PlainTextResponse("", status_code=401)
    s = signals or {}
    title = (s.get("album_title") or "").strip()
    artist = (s.get("album_artist") or "").strip()
    blurb = (s.get("album_blurb") or "").strip()
    if not title or not artist:
        return DatastarResponse([_toast("Album needs a title and an artist", "danger")])
    axid = det_xid("album", artist, title)  # same identity scheme as seed.py
    album = {
        "xid": axid, "title": title, "blurb": blurb or None, "plays": 0,
        # nested write: creates the artist if new, links if it already exists
        "artists": [{"xid": det_xid("artist", artist), "name": artist}],
    }
    try:
        # generic verb + typed MusicAlbumInput dict — async writes need no
        # codegen (the *Input types are the contract, the verb is generic)
        await synthigy.aclient().stack("music_album", album,
                                       acting_as=sess.user.xid)
    except SynthigyError as e:
        return _err_toast(e)
    # Point the page's search at the new title — the write's entity poke
    # arrives on the live stream and refetches with these new signals, so
    # the album appears via the same live path every other tab uses.
    # Nothing here patches the list directly.
    return DatastarResponse([
        SSE.patch_signals({"album_title": "", "album_artist": "",
                           "album_blurb": "", "_adding": False,
                           "q": title, "limit": PER_PAGE, "hits": False}),
        SSE.patch_elements(views.search_meta(title, 1),
                           selector="#albums-meta", mode=Mode.INNER),
        _toast(f"Added “{title}” by {artist}", link=f"/albums/{axid}"),
    ])

@app.post("/albums/{xid}/play")
async def bump_play(request: Request, xid: str):
    sess = _session(request)
    if not sess:
        return PlainTextResponse("", status_code=401)

    try:
        # Ad-hoc get (flat unique-constraint args) → additive stack write.
        c = synthigy.aclient()
        cur = await c.get("music_album", {"xid": xid},
                          {"xid": None, "title": None, "plays": None},
                          acting_as=sess.user.xid)
        if not cur:
            raise SynthigyError("album not found", "NOT_FOUND")
        await c.stack("music_album",
                      {"xid": xid, "plays": (cur.get("plays") or 0) + 1},
                      acting_as=sess.user.xid)
        title = cur.get("title") or "album"
    except SynthigyError as e:
        return _err_toast(e)
    return DatastarResponse([_toast(f"▸ played “{title}” — +1")])


@app.post("/albums/{xid}/tracks")
async def add_track(request: Request, xid: str, signals: ReadSignals):
    sess = _session(request)
    if not sess:
        return PlainTextResponse("", status_code=401)
    s = signals or {}
    title = (s.get("track_title") or "").strip()
    if not title:
        return DatastarResponse([_toast("Track needs a title", "danger")])
    track = {
        # Same deterministic identity the seed uses → re-adding upserts.
        "xid": det_xid("track", xid, title),
        "title": title,
        "length": (s.get("track_length") or "").strip() or None,
        "explicit": bool(s.get("track_explicit")),
        "album": {"xid": xid},          # nested write: link to the parent
    }
    try:
        await synthigy.aclient().stack("music_track", track,
                                       acting_as=sess.user.xid)
    except SynthigyError as e:
        return _err_toast(e)
    return DatastarResponse([
        SSE.patch_signals({"track_title": "", "track_length": "",
                           "track_explicit": False}),
        _toast(f"Added track “{title}”"),
    ])


@app.post("/tracks/{txid}/delete")
async def remove_track(request: Request, txid: str):
    sess = _session(request)
    if not sess:
        return PlainTextResponse("", status_code=401)
    try:
        await synthigy.aclient().delete("music_track", {"xid": txid},
                                        acting_as=sess.user.xid)
    except SynthigyError as e:
        return _err_toast(e)
    return DatastarResponse([_toast("Track removed")])


@app.post("/albums/{xid}/genres")
async def add_genre(request: Request, xid: str, signals: ReadSignals):
    sess = _session(request)
    if not sess:
        return PlainTextResponse("", status_code=401)
    label = ((signals or {}).get("genre_label") or "").strip()
    if not label:
        return DatastarResponse([_toast("Genre needs a label", "danger")])
    try:
        # stack = ADDITIVE: links this genre, leaves the others alone.
        # (sync would REPLACE the album's genre link-set.)
        await synthigy.aclient().stack(
            "music_album",
            {"xid": xid, "genres": [{"xid": det_xid("genre", label),
                                     "label": label}]},
            acting_as=sess.user.xid)
    except SynthigyError as e:
        return _err_toast(e)
    return DatastarResponse([
        SSE.patch_signals({"genre_label": ""}),
        _toast(f"Linked genre “{label}”"),
    ])


@app.post("/albums/{axid}/genres/clear")
async def clear_genres(request: Request, axid: str):
    sess = _session(request)
    if not sess:
        return PlainTextResponse("", status_code=401)
    try:
        # slice = cut a whole relation link-set in one shot (no per-target
        # filtering; entities on both sides survive).
        await synthigy.aclient().slice(
            "music_album", {"xid": {"_eq": axid}},
            {"genres": {"xid": None}}, acting_as=sess.user.xid)
    except SynthigyError as e:
        return _err_toast(e)
    return DatastarResponse([_toast("All genres unlinked")])


@app.post("/albums/{axid}/genres/{gxid}/remove")
async def remove_genre(request: Request, axid: str, gxid: str):
    sess = _session(request)
    if not sess:
        return PlainTextResponse("", status_code=401)

    try:
        # sync REPLACES a relation link-set — write back everything but the
        # one being removed. (Contrast with stack in add_genre: additive.)
        c = synthigy.aclient()
        cur = await c.get("music_album", {"xid": axid},
                          {"xid": None, "genres": {"xid": None}},
                          acting_as=sess.user.xid)
        remaining = [{"xid": g["xid"]} for g in (cur or {}).get("genres") or []
                     if g.get("xid") != gxid]
        await c.sync("music_album", {"xid": axid, "genres": remaining},
                     acting_as=sess.user.xid)
    except SynthigyError as e:
        return _err_toast(e)
    return DatastarResponse([_toast("Genre unlinked")])


@app.post("/albums/{xid}/delete")
async def remove_album(request: Request, xid: str):
    sess = _session(request)
    if not sess:
        return _login_redirect("/albums")
    try:
        # purge deletes everything its selection pulls, not just the
        # matched root: including `tracks` cascades the hard delete to
        # every track under this album, in one round trip.
        await synthigy.aclient().purge(
            "music_album", {"xid": eq(xid)}, {"tracks": {"xid": None}},
            acting_as=sess.user.xid)
    except SynthigyError:
        pass  # land back on the list either way
    return RedirectResponse("/albums", status_code=303)


# ── live streams (one watch per browser page) ────────────────────────────

@app.get("/stream/albums")
async def stream_albums(request: Request):
    sess = _session(request)
    if not sess:
        return PlainTextResponse("", status_code=401)

    async def gen():
        # ONE fixed poke channel per page, on the async client's shared SSE — a
        # suspended coroutine, not a parked thread. It carries NO query
        # window: every entity event patches a poke node whose data-init
        # refetches /albums-search with the browser's CURRENT signals.
        # The stream is never re-pointed, so no connection ever leaks
        # (see views.poke_html for the leak this replaced).
        # ponytail: entity-level pokes cost O(open tabs) refetches per
        # write (server coalesces ~100ms). Fine at demo scale; at real
        # scale, narrow the interest to the visible window's record xids
        # and re-sync it on refetch, like the sync SDK's QueryWatch does.
        watch = synthigy.aclient().watch_entities("music_album")
        n = 0
        try:
            # ready() INSIDE the try: a browser abort mid-bootstrap must
            # still unregister, or the watch leaks on the mux forever.
            await watch.ready()
            # initial poke: catch-up after SSE (re)connects — a cheap
            # idempotent repaint of the current window.
            yield SSE.patch_elements(views.poke_html(n),
                                     selector="#poke", mode=Mode.INNER)
            async for _ev in watch:
                n += 1
                yield SSE.patch_elements(views.poke_html(n),
                                         selector="#poke", mode=Mode.INNER)
        finally:
            await watch.close()

    return DatastarResponse(gen())


@app.get("/stream/albums/{xid}")
async def stream_album(request: Request, xid: str):
    sess = _session(request)
    if not sess:
        return PlainTextResponse("", status_code=401)

    # Direct server-side repaint (not the poke pattern): this stream's
    # query parameters are FIXED for the page's life, so it never needs
    # re-pointing — the albums list only pokes because its window lives
    # in browser signals the server can't see. Same reasoning for
    # /stream/dashboard below.
    async def gen():
        # generated async typed op — same XSQL doc, zero hand-wiring
        watch = MusicAlbumAsync.watch_detail({"xid": xid},
                                             acting_as=sess.user.xid)
        try:
            await watch.ready()  # inside try: abort mid-bootstrap must still close
            rows = watch.list()
            if rows:
                yield SSE.patch_elements(views.album_detail_card(rows[0]),
                                         selector=f"#album-{xid}", mode=Mode.OUTER)
            async for _ev in watch:
                rows = watch.list()
                if rows:
                    yield SSE.patch_elements(views.album_detail_card(rows[0]),
                                             selector=f"#album-{xid}", mode=Mode.OUTER)
        finally:
            await watch.close()

    return DatastarResponse(gen())


def _counter_patches(stats: dict):
    for key, _icon, _label in views.COUNTERS:
        yield SSE.patch_elements(
            views.counter_text(key, stats.get("total_" + key)),
            selector=f"#counter-{key}-val", mode=Mode.INNER)


@app.get("/stream/dashboard")
async def stream_dashboard(request: Request):
    sess = _session(request)
    if not sess:
        return PlainTextResponse("", status_code=401)

    async def gen():
        # ONE stats watch drives both the counters and the chart: it already
        # covers music_album, so on every poke we also refetch the top-N list
        # (an on-demand typed query riding a watch event).
        watch = DashboardAsync.watch_stats(acting_as=sess.user.xid)

        async def paint():
            # sql-template is a single row — AsyncSqlTemplateWatch has .first().
            row = watch.first()
            if row:
                for p in _counter_patches(row):
                    yield p
            top = await MusicAlbumAsync.list({"limit": TOP_N},
                                             acting_as=sess.user.xid)
            yield SSE.patch_elements(views.top_chart_html(top),
                                     selector="#top-chart", mode=Mode.INNER)

        try:
            await watch.ready()  # inside try: abort mid-bootstrap must still close
            async for p in paint():
                yield p
            async for _ev in watch:
                async for p in paint():
                    yield p
        finally:
            await watch.close()

    return DatastarResponse(gen())


# ── OIDC ─────────────────────────────────────────────────────────────────

@app.get("/login")
async def login(returnTo: str = "/"):
    url = auth.start_login(ENDPOINT, CLIENT_ID, REDIRECT_URI, return_to=returnTo)
    return RedirectResponse(url, status_code=302)


@app.get("/auth/callback")
async def callback(code: str = "", state: str = ""):
    if not code or not state:
        return PlainTextResponse("missing code or state", status_code=400)

    async def resolve_user(name):
        return await synthigy.aclient().get("User", {"name": name},
                                            {"xid": None, "name": None})

    try:
        sid, return_to = await auth.complete_login(
            ENDPOINT, CLIENT_ID, CLIENT_SECRET, REDIRECT_URI,
            code, state, resolve_user)
    except auth.AuthError as e:
        return PlainTextResponse(str(e), status_code=e.status)
    resp = RedirectResponse(return_to, status_code=302)
    resp.headers["Set-Cookie"] = auth.cookie_header(sid)
    return resp


@app.api_route("/logout", methods=["GET", "POST"])
async def logout(request: Request):
    auth.drop_session(request.cookies.get(auth.SESSION_COOKIE))
    resp = RedirectResponse("/", status_code=302)
    resp.headers["Set-Cookie"] = auth.clear_cookie_header()
    return resp
