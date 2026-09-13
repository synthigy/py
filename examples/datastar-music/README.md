# datastar-music

A live **music library** for the Synthigy **Python SDK**. A FastAPI BFF renders
server-side HTML over **Tyrell** web components; the browser gets HTML +
**Datastar** SSE patches — no client-side framework, no build step. The mirror of
[`datastar-movies`](../../../js/examples/datastar-movies) (JS) and the
[`chat`](../../../clj/examples/chat) (Clojure) demos, in Python.

```
Browser (Datastar + Tyrell — from CDN, zero build)
   ↕ HTML + SSE patches
FastAPI BFF (this directory)
   ↕ synthigy  (typed ops codegen'd from xsql/music.xsql)
Synthigy server (/data, /oauth, /data/events)
```

**Tokens never touch the browser** (OIDC code+PKCE; cookie-keyed session).
**One async SDK client per process** (`synthigy.aconnect`) → one upstream SSE
BFF→Synthigy, multiplexed across all browser tabs; every read, write, batch,
history call and watch is natively `await`ed on it — no thread pool, no
`asyncio.to_thread`. Every SDK call carries `acting_as=user.xid`.

## What it shows

The demo is a feature tour of the whole SDK — every interactive block carries a
small code chip naming the SDK call behind it.

| page | feature | SDK primitive |
|---|---|---|
| `/` | landing + login state | — |
| `/albums` | live album chart: debounced search, "1M+ plays" switch, growable window, and a sort select (most played / A→Z / Z→A / recently added) driven by an **order param** — `order by ?sort(plays, title, created_on)="plays desc"` declares the default and the sortable set; the server validates the bound value at compile time. Live updates ride ONE fixed entity poke channel per page: each event refetches with the browser's current signals, so changing controls never touches the stream (re-pointing an SSE by DOM swap leaks a browser connection per change) | `watch_entities` + `MusicAlbumAsync.list` |
| `/albums` | ▶ button bumps a play count; every open tab repaints | ad-hoc `get` + generic `stack` |
| `/albums` | "Add album" panel — nested artist write, deterministic artist+title xid (re-creating upserts); the new album appears through the list watch and bumps the dashboard counters live | generic `stack` (typed `MusicAlbumInput`) + nested `music_artist` |
| `/albums/:xid` | live detail — tracks, artists, genres | `MusicAlbumAsync.watch_detail` + `MusicAlbumAsync.detail` |
| `/albums/:xid` | add a track (nested write linking to the album, deterministic xid → idempotent) | generic `stack` (typed `MusicTrackInput`) |
| `/albums/:xid` | remove a track (soft delete) / delete the album (modal confirm) | generic `delete` |
| `/albums/:xid` | genre chips: add = additive link, dismiss × = replace link-set minus one, clear = cut the whole link-set | `stack` vs `sync` vs `synthigy.slice` |
| `/albums/:xid` | change-history timeline (renders only when the server has an audit provider + the model has audit enabled) | `synthigy.history().events` |
| `/dashboard` | first paint = counters + top-N chart in ONE wire request | `overview_async()` (`@batch`) |
| `/dashboard` | live counters + chart riding one SQL-template watch over three entities | `DashboardAsync.watch_stats` + `MusicAlbumAsync.list` |
| `/dashboard` | "Billion club" — ad-hoc data-shaped search with a filter helper | `synthigy.search` + `gt()` |
| covers | real album art on every tile, backfilled from Spotify oEmbed + Deezer (no API keys) | `backfill_covers.py`: paged `search` + batched `sync` |

Live updates are **notify-then-refetch**: an event just pokes; the BFF re-runs
the RLS-scoped query and repaints the fragment. Writes never repaint anything
themselves — the watch does. Open `/albums` in two tabs; press ▶ in one (or
change an album from the REPL) and the other updates in ~100ms.

UI interactions are pure **Tyrell + Datastar**: `ty-switch` for the plays
filter, `ty-checkbox` for explicit tracks, dismissible `ty-tag`s for genre
links, `ty-modal` for the delete confirm, `ty-tooltip` everywhere (Tyrell
events carry their payload in `evt.detail`).

## Prerequisites

1. **Synthigy server** with `:synthigy/server` up (IAM + subscriptions), default
   `http://localhost:7887`.
2. **JWT keypair seeded** (one-time, REPL): `(require '[synthigy.iam.encryption :as enc]) (enc/rotate-keypair)`
3. **Python ≥ 3.10.**

## Setup

### 1. Install deps (in a venv)

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt   # fastapi, uvicorn, datastar-py
pip install -e ../..              # the synthigy SDK (stdlib-only)
```

### 2. Deploy the dataset (REPL)

The ERD is Music Album → tracks / artists / genres. Deploy it from the server REPL by
loading the builder — it creates the tables and writes the
`datasets/synthigy-music@0.1.0.json` artifact:

```clojure
(load-file "sdk/py/examples/datastar-music/datasets/build.clj")
```

Re-deploying? Destroy the old one first:
`(require '[synthigy.dataset :as ds]) (ds/destroy! {:name "Synthigy Music"})`.

Once exported, teammates can redeploy the JSON directly:
`(ds/deploy! (transit/<-transit (slurp "…/datasets/synthigy-music@0.1.0.json")))`.

### 3. Register an OAuth client + a demo user (REPL)

```clojure
(require '[synthigy.iam :as iam] '[synthigy.dataset :as dataset])
;; Confidential client (authorization_code). Save the printed secret — shown once.
(let [{:keys [id secret]}
      (iam/add-client
        {:id "datastar-music" :name "Datastar Music Demo" :type :confidential
         :settings {:allowed-grants ["authorization_code"] :trusted true
                    :scopes ["openid" "email" "profile"]
                    :redirect-uris ["http://localhost:5175/auth/callback"]
                    :redirections ["http://localhost:5175/auth/callback"]}})]
  (println "CLIENT_ID:" id "\nCLIENT_SECRET:" secret))

;; A demo user with read/write (SUPERUSER role is simplest for a demo).
(dataset/sync-entity :iam/user
  {:name "demo" :password "demo" :active true :type :PERSON
   :roles [{:xid "CsRfQHNu3RyCgbpQQdanbd"}]})   ; SUPERUSER

;; Let the confidential client's service user seed/write too (SUPERUSER):
(dataset/sync-entity :iam/user
  {:name "datastar-music" :roles [{:xid "CsRfQHNu3RyCgbpQQdanbd"}]})
```

> The SUPERUSER role xid above is from this repo's dev server; confirm yours with
> `(filter #(= "SUPERUSER" (:name %)) (dataset/search-entity :iam/user-role {} {:name nil :xid nil}))`.

### 4. Seed data

```bash
SYNTHIGY_CLIENT_ID=datastar-music SYNTHIGY_CLIENT_SECRET=<secret> python seed.py
```

`seed.py` uses 6 curated albums out of the box — zero downloads, runs immediately.

**Album covers** (optional, after seeding):

```bash
SYNTHIGY_CLIENT_ID=… SYNTHIGY_CLIENT_SECRET=… python backfill_covers.py
```

Fills `Music Album.Cover` with artwork URLs — Spotify oEmbed for albums whose
`link` points at open.spotify.com (exact), Deezer search for the rest
(artist+title, keyless, rate-limit friendly). Idempotent: albums with a cover
are skipped (`--force` refetches), misses stay empty and the UI falls back to
a gray tile. `--limit N` for a quick top-N pass.

**Want real volume?** Point it at a real dataset instead, the movies way
(`prep_movielens.bb` → sync). Download the Kaggle
[Spotify Tracks Dataset](https://www.kaggle.com/datasets/maharshipandya/spotify-tracks-dataset)
(`dataset.csv`, ~114k tracks) and reshape it:

```bash
bb datasets/prep_spotify.bb /path/to/dataset.csv --top 300
```

That writes `datasets/{artists,genres,albums}.json` (albums ranked by
popularity, tracks deduped, `popularity → plays`). `seed.py` auto-detects those
files and seeds from them instead of the curated set. Tweak `--top`, or edit the
JSON, then re-run `seed.py`.

**Identity is by deterministic xid, not a unique title** — exactly how movies
bakes ids in `bake_xid.clj`. Each album's xid is derived from `artist + title`
(a faithful port of `id/uuid->nanoid`, verified against the server), so two
different "Discovery" albums are two distinct records, `Music Album.Title` needs no
unique constraint, and re-running `seed.py` upserts by id instead of duplicating.
Artists and genres key off their name/label the same way.

### 5. Codegen the typed ops

```bash
SYNTHIGY_CLIENT_ID=datastar-music SYNTHIGY_CLIENT_SECRET=<secret> \
  python -m synthigy.codegen gen xsql/music.xsql --out generated/music_gen.py
```

⚠️ After editing `xsql/music.xsql`, re-run `gen` — the server recompiles
the XSQL into IR and regenerates `generated/music_gen.py`.

## Run

```bash
export SYNTHIGY_CLIENT_ID=datastar-music SYNTHIGY_CLIENT_SECRET=<secret>
uvicorn serve:app --port 5175 --reload --timeout-graceful-shutdown 3
```

> `--timeout-graceful-shutdown` matters with `--reload`: page-lifetime SSE
> streams never end on their own, and uvicorn's default graceful shutdown
> waits for them forever — so any file edit while a browser tab is open
> wedges the reload permanently. The timeout force-closes lingering
> streams after 3s instead (Datastar reconnects them).

Open <http://localhost:5175> and log in as `demo` / `demo`.

| var | default | meaning |
|---|---|---|
| `SYNTHIGY_ENDPOINT` | `http://localhost:7887` | Synthigy base URL |
| `SYNTHIGY_CLIENT_ID` / `_SECRET` | (required) | OAuth client creds |
| `PORT` | `5175` | BFF port |
| `BASE_URL` | `http://localhost:$PORT` | OAuth `redirect_uri` base |

Change `PORT`/`BASE_URL` → update the client's `redirect-uris` to match.

## Files

```
serve.py                 — FastAPI BFF: routes, OIDC, SSE streams
auth.py                  — OIDC code+PKCE + in-memory sessions (stdlib only)
views.py                 — HTML fragments (Tyrell + Datastar, CDN)
icons.py                 — inline SVG icon helper
seed.py                  — seed script (dogfoods the blocking facade)
backfill_covers.py       — album-art backfill (paged search + batched sync)
xsql/music.xsql          — the data layer (list / detail / stats)
generated/music_gen.py   — AUTO-GENERATED typed ops (do not edit; git-ignored)
datasets/build.clj       — builds + deploys the ERD, writes the JSON artifact
datasets/prep_spotify.bb — reshapes a Kaggle Spotify CSV → seedable JSON (optional)
```

## Verifying (e2e)

```bash
pip install playwright && playwright install chromium
python e2e.py       # drives a real browser through every feature above
```

Sixteen checks: login, live list, debounced search, sort select, hits
switch, play-bump watch repaint, track add/remove, genre stack/sync,
album create + deterministic-xid dedup, and live dashboard counters
across a second page. Self-cleaning (creations are deleted through the
UI); exits non-zero on failure.

## Not in this demo (on purpose)

- `purge` (search-and-destroy) and the tree ops (`search_tree`/`get_tree` — the
  model has no self-referential relation).
- Production session store (sessions are mirrored to `.sessions.json` so
  `--reload` doesn't log you out — swap for Redis/Postgres in real life) /
  `id_token` signature verification.
