"""Backfill Music Album.Cover with real artwork URLs.

Sources, in order:
  1. Spotify oEmbed  — exact match for albums whose `link` points at
     open.spotify.com/album/… (the curated seed set). No auth.
  2. Deezer search   — artist+title lookup for everything else. No auth,
     ~50 requests / 5 s allowed; we stay well under.

Dogfoods the SDK both ways: pages through albums with an ad-hoc
data-shaped `search` (relation pull for artist names), writes back with
batched `sync` upserts. Idempotent — albums that already have a cover are
skipped, misses just stay empty (the UI falls back to a gray tile).

    SYNTHIGY_CLIENT_ID=… SYNTHIGY_CLIENT_SECRET=… python backfill_covers.py
Options: --force (refetch even if cover set), --limit N (stop after N albums)
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import synthigy

ENDPOINT = os.environ.get("SYNTHIGY_ENDPOINT", "http://localhost:7887")
PAGE = 200          # albums per SDK search page
BATCH = 100         # cover updates per sync call
DEEZER_DELAY = 0.15  # seconds between Deezer calls (limit is 50 per 5s)

FORCE = "--force" in sys.argv
LIMIT = None
if "--limit" in sys.argv:
    LIMIT = int(sys.argv[sys.argv.index("--limit") + 1])


def _get_json(url: str) -> dict | None:
    req = urllib.request.Request(url, headers={"User-Agent": "datastar-music-demo"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, ValueError, OSError):
        return None


def spotify_cover(link: str | None) -> str | None:
    if not link or "open.spotify.com/album/" not in link:
        return None
    data = _get_json("https://open.spotify.com/oembed?url="
                     + urllib.parse.quote(link, safe=""))
    return (data or {}).get("thumbnail_url")


def _clean_title(title: str) -> str:
    # "Nevermind (Super Deluxe Edition)" → "Nevermind"; drop bracketed and
    # " - 2011 Remaster"-style suffixes that sink fuzzy search hit rate.
    t = re.sub(r"[(\[][^)\]]*[)\]]", " ", title)
    t = re.sub(r"\s+-\s+.*$", " ", t)
    return re.sub(r"\s+", " ", t).strip() or title


def deezer_cover(artist: str | None, title: str) -> str | None:
    queries = []
    if artist:
        queries.append(f'artist:"{artist}" album:"{_clean_title(title)}"')
    queries.append(f"{artist or ''} {_clean_title(title)}".strip())
    for q in queries:
        time.sleep(DEEZER_DELAY)
        data = _get_json("https://api.deezer.com/search/album?limit=1&q="
                         + urllib.parse.quote(q))
        rows = (data or {}).get("data") or []
        if rows and rows[0].get("cover_big"):
            return rows[0]["cover_big"]
    return None


def main() -> None:
    cid = os.environ.get("SYNTHIGY_CLIENT_ID")
    secret = os.environ.get("SYNTHIGY_CLIENT_SECRET")
    if not cid or not secret:
        raise SystemExit("SYNTHIGY_CLIENT_ID and SYNTHIGY_CLIENT_SECRET are required")
    synthigy.connect(ENDPOINT, client_id=cid, client_secret=secret)

    done = hits = misses = 0
    updates: list[dict] = []

    def flush():
        nonlocal updates
        if updates:
            synthigy.sync("music_album", updates)
            updates = []

    offset = 0
    while True:
        page = synthigy.search(
            "music_album",
            {"_limit": PAGE, "_offset": offset, "_order_by": {"plays": "desc"}},
            {"xid": None, "title": None, "link": None, "cover": None,
             "artists": {"name": None}})
        if not page:
            break
        for a in page:
            if LIMIT is not None and done >= LIMIT:
                flush()
                print(f"stopped at --limit {LIMIT}: {hits} covers, {misses} misses")
                return
            done += 1
            if a.get("cover") and not FORCE:
                continue
            artist = next((ar.get("name") for ar in a.get("artists") or []), None)
            cover = spotify_cover(a.get("link")) or deezer_cover(artist, a["title"])
            if cover:
                hits += 1
                updates.append({"xid": a["xid"], "cover": cover})
                if len(updates) >= BATCH:
                    flush()
            else:
                misses += 1
            if done % 100 == 0:
                print(f"  {done} scanned · {hits} covers · {misses} misses", flush=True)
        offset += PAGE
    flush()
    print(f"done: {done} scanned, {hits} covers set, {misses} without a match")


if __name__ == "__main__":
    main()
