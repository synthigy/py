"""End-to-end verification of the datastar-music demo — drives a real
browser through every SDK feature the demo showcases.

    python e2e.py            # runs all sections, exits non-zero on failure

Prereqs: the BFF running (README §Run), seeded data, and
    pip install playwright && playwright install chromium

Covers: login (OIDC PKCE) · live album list + debounced search + sort
select + hits switch (typed XSQL params incl. `order by ?sort`) · play
bump / add+remove track / genre stack-sync-slice trio (writes repaint via
watches, never directly) · album create with deterministic-xid dedup ·
dashboard batch first-paint + live counters across a second page.

Idempotent-ish: everything it creates it deletes through the UI. The
deterministic-xid upserts mean re-runs never duplicate; a test artist/genre
ENTITY may linger unlinked (no artist-delete UI) — harmless, reused next run.

Env: E2E_BASE_URL (default http://localhost:5175),
     E2E_USER / E2E_PASS (default demo / demo).
"""

from __future__ import annotations

import asyncio
import os
import sys

from playwright.async_api import async_playwright

BASE = os.environ.get("E2E_BASE_URL", "http://localhost:5175").rstrip("/")
USER = os.environ.get("E2E_USER", "demo")
PASS = os.environ.get("E2E_PASS", "demo")

FAILURES: list[str] = []


def check(ok, label):
    print(("  ok   " if ok else "  FAIL ") + label)
    if not ok:
        FAILURES.append(label)


async def login(pg, path="/albums"):
    await pg.goto(BASE + path)
    if await pg.locator("#username").count():
        await pg.fill("#username", USER)
        await pg.fill("#password", PASS)
        await pg.click("button")
    await pg.wait_for_url(f"**{path.split('?')[0]}**", timeout=15000)
    await pg.wait_for_timeout(2500)  # stream connect + ty-ready


async def pick_sort(pg, label):
    await pg.locator("ty-select").click()
    await pg.wait_for_timeout(400)
    await pg.locator("ty-option", has_text=label).click()
    await pg.wait_for_timeout(1800)


async def first_title(pg):
    return (await pg.locator("#albums li .truncate").first.inner_text()).strip()


async def section_list_and_sort(pg):
    print("— list / search / sort / filter")
    check(await pg.locator("#albums li").count() > 0, "albums render")

    default_first = await first_title(pg)
    await pick_sort(pg, "Title A→Z")
    az_first = await first_title(pg)
    await pick_sort(pg, "Title Z→A")
    za_first = await first_title(pg)
    check(az_first != za_first, f"sort reorders (A→Z {az_first!r} vs Z→A {za_first!r})")
    await pick_sort(pg, "Most played")
    check(await first_title(pg) == default_first, "sort returns to default order")

    reqs = []
    pg.on("request", lambda r: "/albums-search" in r.url and reqs.append(r.url))
    await pg.locator("ty-input[data-bind=q] input").click()
    await pg.locator("ty-input[data-bind=q] input").type("nevermind", delay=60)
    await pg.wait_for_timeout(1500)
    check(len(reqs) == 1, f"debounce: {len(reqs)} request for 9 keystrokes")
    check("nevermind" in (await first_title(pg)).lower(), "search filters")
    await pg.locator("ty-input[data-bind=q] input").fill("")
    await pg.wait_for_timeout(1500)

    before = await pg.locator("#albums li").count()
    await pg.locator("ty-switch").click()
    await pg.wait_for_timeout(1800)
    hits = await pg.locator("#albums li").count()
    check(hits < before, f"1M+ switch narrows ({before} → {hits})")
    await pg.locator("ty-switch").click()
    await pg.wait_for_timeout(1800)


async def section_live_write(pg):
    print("— live write (play bump via watch repaint)")
    tip = pg.locator("#albums li ty-tag[flavor=rating] ty-tooltip").first
    before = (await tip.text_content() or "").strip()
    await pg.locator("#albums li ty-button").first.click()
    await pg.wait_for_timeout(2500)
    after = (await tip.text_content() or "").strip()
    check(before != after and pg.url.endswith("/albums"),
          f"exact plays repaint in place ({before!r} → {after!r})")


async def section_detail(pg):
    print("— detail: tracks + genres (stack / sync / slice)")
    await pg.locator("#albums li a.row-link").first.click()
    await pg.wait_for_selector("ty-input[data-bind=track_title]", timeout=10000)
    await pg.wait_for_timeout(2000)

    await pg.locator("ty-switch").click()   # explicit-lyrics slide toggle
    await pg.locator("ty-input[data-bind=track_title] input").fill("E2E Track")
    await pg.locator("ty-input[data-bind=track_length] input").fill("1:11")
    await pg.locator("div:has(> ty-input[data-bind=track_title]) > ty-button").click()
    await pg.wait_for_timeout(2200)
    row = pg.locator("div.row", has_text="E2E Track")
    check(await row.count() == 1, "track added (nested write)")
    # quiet "E" badge (its tooltip text is unique within the row)
    check(await row.locator("ty-tag", has_text="explicit lyrics").count() == 1,
          "explicit badge (ty-switch)")
    await row.locator("ty-button").click()
    await pg.wait_for_timeout(2200)
    check(await pg.locator("div.row", has_text="E2E Track").count() == 0, "track removed (delete)")

    await pg.locator("ty-input[data-bind=genre_label] input").fill("E2E Genre")
    await pg.locator("div:has(> ty-input[data-bind=genre_label]) > ty-button").first.click()
    await pg.wait_for_timeout(2200)
    chip = pg.locator("ty-tag[dismissible]", has_text="E2E Genre")
    check(await chip.count() == 1, "genre linked (stack, additive)")
    await chip.locator("button").click()
    await pg.wait_for_timeout(2200)
    check(await pg.locator("ty-tag[dismissible]", has_text="E2E Genre").count() == 0,
          "genre unlinked (sync replaces link-set)")


async def section_create_and_dashboard(ctx):
    print("— create album + live dashboard (two pages)")
    dash = await ctx.new_page()
    await login(dash, "/dashboard")
    await dash.wait_for_selector("#counter-albums-val", timeout=10000)
    await dash.wait_for_timeout(1500)
    albums0 = await dash.locator("#counter-albums-val").inner_text()
    check(albums0 not in ("", "…"), f"dashboard batch first paint (albums={albums0})")

    alb = await ctx.new_page()
    await login(alb, "/albums")
    await alb.locator("ty-button", has_text="Add album").click()
    await alb.wait_for_timeout(400)
    await alb.locator("ty-input[data-bind=album_title] input").fill("E2E Verification Album")
    await alb.locator("ty-input[data-bind=album_artist] input").fill("E2E Band")
    await alb.locator("ty-button", has_text="Create").click()
    await alb.wait_for_timeout(3000)
    check(await alb.locator("#albums li", has_text="E2E Verification Album").count() == 1,
          "created album arrives via the list watch")
    albums1 = await dash.locator("#counter-albums-val").inner_text()
    check(albums0 != albums1, f"dashboard counter live-bumped ({albums0} → {albums1})")

    # delete it through the UI (modal confirm) — leaves the catalog clean
    await alb.locator("#albums li a.row-link", has_text="E2E Verification Album").click()
    await alb.wait_for_selector("ty-button:has-text('Delete album')", timeout=10000)
    await alb.locator("ty-button", has_text="Delete album").click()
    await alb.wait_for_timeout(600)
    await alb.locator("#delete-modal form ty-button").click()
    await alb.wait_for_url("**/albums", timeout=10000)
    await alb.wait_for_timeout(2500)
    albums2 = await dash.locator("#counter-albums-val").inner_text()
    check(albums2 == albums0, f"deleted; counter back to {albums0}")
    await dash.close()
    await alb.close()


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx = await browser.new_context(viewport={"width": 1280, "height": 900})
        pg = await ctx.new_page()
        pg.on("dialog", lambda d: asyncio.ensure_future(d.accept()))

        await login(pg)
        await section_list_and_sort(pg)
        await section_live_write(pg)
        await section_detail(pg)
        await pg.close()
        await section_create_and_dashboard(ctx)
        await browser.close()

    print(f"\n{'PASS' if not FAILURES else 'FAIL'} — {len(FAILURES)} failure(s)")
    for f in FAILURES:
        print("  ✗", f)
    sys.exit(1 if FAILURES else 0)


if __name__ == "__main__":
    asyncio.run(main())
