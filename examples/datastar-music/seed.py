"""Seed the Synthigy Music dataset with a handful of well-known albums.

Dogfoods the Python SDK (same `synthigy.sync` the BFF uses). Run once after
deploying the dataset:

    SYNTHIGY_CLIENT_ID=… SYNTHIGY_CLIENT_SECRET=… python seed.py

The client (or the acting_as user) needs write on Music Album/Music Track/
Music Artist/Music Genre —
granting the service user the SUPERUSER role is simplest (README). Set
SYNTHIGY_SEED_ACTING_AS to a user xid to seed on their behalf.
"""

from __future__ import annotations

import hashlib
import json
import os

import synthigy

ENDPOINT = os.environ.get("SYNTHIGY_ENDPOINT", "http://localhost:7887")
ACTING_AS = os.environ.get("SYNTHIGY_SEED_ACTING_AS")  # optional user xid
HERE = os.path.dirname(os.path.abspath(__file__))

# Deterministic xid from a natural key — port of synthigy.dataset.id/uuid->nanoid
# (UUID v3 over the key, then Base58). Same identity movies bakes into its import:
# lets Music Album.Title stay non-unique while re-seeds upsert instead of duplicating.
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def xid(kind: str, *parts: str) -> str:
    d = bytearray(hashlib.md5(("synthigy-music/" + kind + "/" + "/".join(parts))
                              .encode("utf-8")).digest())
    d[6] = (d[6] & 0x0f) | 0x30   # UUID v3
    d[8] = (d[8] & 0x3f) | 0x80   # IETF variant
    n, s = int.from_bytes(bytes(d), "big"), ""
    while n > 0:
        n, r = divmod(n, 58)
        s = _B58[r] + s
    return "1" * (22 - len(s)) + s

ARTISTS = [
    {"name": "Pink Floyd", "country": "UK"},
    {"name": "Michael Jackson", "country": "USA"},
    {"name": "Fleetwood Mac", "country": "UK/USA"},
    {"name": "Daft Punk", "country": "France"},
    {"name": "Kendrick Lamar", "country": "USA"},
    {"name": "Radiohead", "country": "UK"},
]

GENRES = [
    {"label": "Rock"},
    {"label": "Pop"},
    {"label": "Electronic"},
    {"label": "Hip-Hop"},
    {"label": "Alternative"},
    {"label": "Progressive"},
]

ALBUMS = [
    {"title": "The Dark Side of the Moon",
     "blurb": "Concept album on conflict, greed and time.",
     "plays": 4_100_000_000, "link": "https://open.spotify.com/album/4LH4d3cOWNNsVw41Gqt2kv",
     "artists": ["Pink Floyd"], "genres": ["Rock", "Progressive"],
     "tracks": [
         {"title": "Speak to Me", "release_on": "1973-03-01", "length": "1:30"},
         {"title": "Breathe (In the Air)", "release_on": "1973-03-01", "length": "2:43"},
         {"title": "Money", "release_on": "1973-03-01", "length": "6:22"}]},
    {"title": "Thriller",
     "blurb": "Best-selling album of all time.",
     "plays": 3_400_000_000, "link": "https://open.spotify.com/album/2ANVost0y2y52ema1E9xAZ",
     "artists": ["Michael Jackson"], "genres": ["Pop"],
     "tracks": [
         {"title": "Wanna Be Startin' Somethin'", "release_on": "1982-11-30", "length": "6:03"},
         {"title": "Thriller", "release_on": "1982-11-30", "length": "5:57"},
         {"title": "Beat It", "release_on": "1982-11-30", "length": "4:18"}]},
    {"title": "Rumours",
     "blurb": "Breakup record turned soft-rock landmark.",
     "plays": 1_900_000_000, "link": "https://open.spotify.com/album/1bt6q2SruMsBtcerNVtpZB",
     "artists": ["Fleetwood Mac"], "genres": ["Rock", "Pop"],
     "tracks": [
         {"title": "Dreams", "release_on": "1977-02-04", "length": "4:14"},
         {"title": "Go Your Own Way", "release_on": "1977-02-04", "length": "3:38"}]},
    {"title": "Random Access Memories",
     "blurb": "Grammy Album of the Year, live disco revival.",
     "plays": 2_600_000_000, "link": "https://open.spotify.com/album/4m2880jivSbbyEGAKfudGh",
     "artists": ["Daft Punk"], "genres": ["Electronic", "Pop"],
     "tracks": [
         {"title": "Give Life Back to Music", "release_on": "2013-05-17", "length": "4:35"},
         {"title": "Get Lucky", "release_on": "2013-05-17", "length": "6:07"}]},
    {"title": "To Pimp a Butterfly",
     "blurb": "Jazz-inflected meditation on race and fame.",
     "plays": 2_200_000_000, "link": "https://open.spotify.com/album/7ycBtnsMtyVbbwTfJwRjSP",
     "artists": ["Kendrick Lamar"], "genres": ["Hip-Hop"],
     "tracks": [
         {"title": "King Kunta", "release_on": "2015-03-15", "length": "3:54", "explicit": True},
         {"title": "Alright", "release_on": "2015-03-15", "length": "3:39", "explicit": True}]},
    {"title": "OK Computer",
     "blurb": "Anxiety, alienation and the machine age.",
     "plays": 1_500_000_000, "link": "https://open.spotify.com/album/6dVIqQ8qmQ5GBnJ9shOYGE",
     "artists": ["Radiohead"], "genres": ["Alternative", "Rock"],
     "tracks": [
         {"title": "Airbag", "release_on": "1997-05-21", "length": "4:44"},
         {"title": "Paranoid Android", "release_on": "1997-05-21", "length": "6:23"}]},
]


def _curated():
    """The curated albums above, stamped with the SAME deterministic xids the
    prep script emits — so identity (and idempotency) is uniform across modes."""
    artists = [{"xid": xid("artist", a["name"]), **a} for a in ARTISTS]
    genres = [{"xid": xid("genre", g["label"]), **g} for g in GENRES]
    albums = []
    for alb in ALBUMS:
        primary = alb["artists"][0]
        axid = xid("album", primary, alb["title"])
        albums.append({
            "xid": axid, "title": alb["title"], "blurb": alb["blurb"],
            "plays": alb["plays"], "link": alb["link"],
            "artists": [{"xid": xid("artist", n)} for n in alb["artists"]],
            "genres": [{"xid": xid("genre", g)} for g in alb["genres"]],
            "tracks": [{"xid": xid("track", axid, t["title"]),
                        "explicit": False, **t} for t in alb["tracks"]],
        })
    return artists, genres, albums


def _load():
    """Prefer prepped JSON from `bb prep_spotify.bb` (real dataset); else the
    curated set so the demo runs with zero download. Both carry xids + link
    relations by xid — sync upserts on the id, no unique business key needed."""
    fp = os.path.join(HERE, "datasets", "albums.json")
    if not os.path.exists(fp):
        return (*_curated(), "curated")
    load = lambda f: json.load(open(os.path.join(HERE, "datasets", f)))
    return load("artists.json"), load("genres.json"), load("albums.json"), "prepped"


def main():
    synthigy.connect(ENDPOINT,
                     client_id=os.environ["SYNTHIGY_CLIENT_ID"],
                     client_secret=os.environ["SYNTHIGY_CLIENT_SECRET"])
    opts = {"acting_as": ACTING_AS} if ACTING_AS else {}

    artists, genres, albums, source = _load()
    print(f"seeding from {source}: {len(albums)} albums")

    # Records carry their xids; relations reference artist/genre xids. Sync the
    # reference tables first so those ids exist, then the albums.
    synthigy.sync("music_artist", artists, **opts)
    synthigy.sync("music_genre", genres, **opts)
    print(f"seeded {len(artists)} artists, {len(genres)} genres")

    # For a handful of curated albums, log each title; for a prepped dataset
    # (thousands), that would spam thousands of lines — log progress instead.
    chatty = len(albums) <= 20
    for i, alb in enumerate(albums, 1):
        synthigy.sync("music_album", alb, **opts)
        if chatty:
            print(f"  seeded {alb['title']} ({len(alb['tracks'])} tracks)")
        elif i % 250 == 0 or i == len(albums):
            print(f"  seeded {i}/{len(albums)} albums…")

    print("done.")


if __name__ == "__main__":
    main()
