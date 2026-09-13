"""HTML fragment builders — Tyrell web components + Datastar, all from CDN
(zero browser build). The BFF renders these server-side and patches them
into the page over SSE. Colours ride Tyrell's `--ty-*` tokens; layout is
Tailwind utilities; a small <style> block carries the demo-specific edges.

Every interactive block carries a small `sdk` chip naming the SDK primitive
that powers it — the page doubles as a feature tour.
"""

from __future__ import annotations

import hashlib
import html as _html
import json as _json

from icons import icon

TYRELL = "1.0.0-TC31"  # "TC" = dev/test channel, pinned here to try unreleased features; "RC" = stable release-candidate track (npm `latest`)
DATASTAR = "v1.0.2"

PER_PAGE = 20
HITS_MIN_PLAYS = 1_000_000  # "hits only" threshold for the ?min_plays param


def esc(s) -> str:
    return "" if s is None else _html.escape(str(s), quote=True)


def fmt_plays(n) -> str:
    # `plays` mixes two units — real streaming counts (curated albums, up to
    # billions) and a Kaggle popularity sum (0-100 per track, low thousands).
    # A raw comma-grouped int both overflows the dashboard tile at 10+ digits
    # AND makes the two look deceptively comparable. SI-suffix both scales.
    if n is None:
        return "—"
    n = int(n)
    a = abs(n)
    if a >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if a >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if a >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def art(seed: str | None, cls: str = "art-sm", icon_size: str = "md",
        cover: str | None = None) -> str:
    """Album-art tile. Real cover image when the record has one (backfilled
    from Spotify/Deezer — see backfill_covers.py); otherwise a neutral gray
    gradient where the hash only nudges lightness, so tiles stay
    distinguishable without colour. A broken image URL removes itself and
    falls back to the gradient underneath."""
    h = int(hashlib.md5((seed or "?").encode()).hexdigest()[:10], 16)
    top = 26 + h % 12          # 26–37% lightness
    bot = 12 + (h >> 8) % 6    # 12–17%
    hue = 210 + (h >> 16) % 40  # cool gray drift, near-zero saturation
    img = (f'<img src="{esc(cover)}" alt="" loading="lazy" '
           f'onerror="this.remove()">') if cover else ""
    return (f'<div class="art {cls}" style="background:'
            f'linear-gradient(135deg,hsl({hue} 6% {top}%),hsl({hue} 8% {bot}%))">'
            f'{img}{icon("disc", icon_size)}</div>')


def sdk_chip(label: str, tip: str = "Python SDK call behind this block") -> str:
    """Tiny code chip naming the SDK primitive powering a UI block."""
    return (f'<code class="sdkchip">{esc(label)}'
            f'<ty-tooltip placement="top">{esc(tip)}</ty-tooltip></code>')


def tooltip(text: str, placement: str = "top") -> str:
    """Nested ty-tooltip — attaches to its PARENT element."""
    return f'<ty-tooltip placement="{placement}">{esc(text)}</ty-tooltip>'


def poke_html(key) -> str:
    """A poke: a fresh node whose data-init re-runs the albums refresh with
    the browser's CURRENT signals ($q/$limit/$hits/$sort). The live stream
    patches one of these per entity event — the page's ONE SSE connection
    never changes, so nothing leaks. (Re-pointing the stream by DOM swap
    does NOT abort the old fetch in this Datastar version: each sort/toggle
    change leaked one live SSE connection until the browser's 6-per-host
    limit starved every request — reproduced with Playwright, +1 per
    change, page dead at +6.)"""
    return f'<div id="pk-{key}" data-init="{_REFRESH}"></div>'


def toast_html(msg: str, flavor: str = "success",
               link: str | None = None, link_text: str = "view") -> str:
    ic = {"success": "zap", "danger": "ban"}.get(flavor, "info")
    extra = (f' <a href="{esc(link)}" class="font-semibold underline">'
             f'{esc(link_text)} →</a>') if link else ""
    return (f'<div class="toast-msg">'
            f'<ty-tag flavor="{flavor}" size="md">{icon(ic, "sm", "start")}'
            f'{esc(msg)}{extra}</ty-tag></div>')


# ── shell ────────────────────────────────────────────────────────────────

_NAV = [("/albums", "disc", "Albums"), ("/dashboard", "chart", "Dashboard")]

_STYLE = """
  :root { --ty-brand-hue: 95; --ty-brand-chroma: 0.15;
          --font-body: "DM Sans", ui-sans-serif, system-ui, sans-serif;
          --font-mono: "JetBrains Mono", ui-monospace, "SF Mono", Menlo, monospace; }
  /* Yellow needs lifting or it reads olive; black text on the solid fill. */
  html[data-theme="dark"] {
    --ty-solid-primary-fg: black;
    --ty-solid-primary: oklch(0.88 0.17 95);
    --ty-solid-primary-hover: oklch(0.83 0.18 95);
    --ty-solid-primary-active: oklch(0.78 0.18 95);
    --ty-color-primary: oklch(0.86 0.15 95);
  }
  * { box-sizing: border-box; }
  html:not(.ty-ready) body { opacity: 0; }
  body { background: var(--ty-surface-canvas); color: var(--ty-text);
         font: 14px/1.55 var(--font-body);
         -webkit-font-smoothing: antialiased;
         transition: opacity .12s ease; }
  a { color: var(--ty-color-primary); text-decoration: none; }
  a:hover { text-decoration: underline; text-underline-offset: 3px; }
  code { background: var(--ty-surface-elevated); padding: 2px 6px; border-radius: 4px;
         font: 500 .88em/1 var(--font-mono); color: var(--ty-color-primary); }
  .sdkchip { font-size: .68rem; padding: 2px 7px; border: 1px solid var(--ty-border);
             color: var(--ty-text-faint); white-space: nowrap;
             font-family: var(--font-mono);
             text-transform: none; letter-spacing: 0; }
  .surface { background: var(--ty-surface-content); border: 1px solid var(--ty-border); }
  .topbar { background: var(--ty-surface-content); border-bottom: 1px solid var(--ty-border); }
  .brand { color: var(--ty-text); }
  .brand:hover { text-decoration: none; }
  .brand ty-icon { color: var(--ty-color-primary); }
  .nav-link { color: var(--ty-text-faint); transition: background .15s, color .15s; }
  .nav-link ty-icon { opacity: .75; }
  .nav-link:hover { color: var(--ty-text); background: var(--ty-surface-elevated); text-decoration: none; }
  .nav-link[aria-current="page"] { color: var(--ty-color-primary);
    background: var(--ty-bg-primary-soft); font-weight: 600; }
  .nav-link[aria-current="page"] ty-icon { opacity: 1; }
  .live-badge { display: inline-flex; align-items: center; gap: 6px; padding: 4px 9px;
                border: 1px solid var(--ty-border); border-radius: 999px;
                font: 500 10px/1 var(--font-mono); letter-spacing: .06em;
                color: var(--ty-text-faint); cursor: help; }
  .live-dot { width: 6px; height: 6px; border-radius: 50%; background: var(--ty-color-success);
              animation: live-pulse 1.6s ease-in-out infinite; }
  @keyframes live-pulse {
    0%, 100% { box-shadow: 0 0 4px var(--ty-color-success), 0 0 0 0 rgba(74,222,128,.45); }
    50% { box-shadow: 0 0 6px var(--ty-color-success), 0 0 0 4px rgba(74,222,128,0); } }
  /* One quiet gold for play counts — the only coloured tag among neutrals. */
  ty-tag[flavor="rating"] { --tag-bg: oklch(0.26 0.035 92); --tag-color: oklch(0.80 0.10 92); --tag-border-color: oklch(0.40 0.06 92); }
  .row { border-bottom: 1px solid var(--ty-border-soft); }
  .row:last-child { border-bottom: 0; }
  .row-link { color: inherit; transition: background .15s; position: relative; }
  .row-link::before { content: ""; position: absolute; left: 0; top: 0; bottom: 0;
                      width: 2px; background: transparent; transition: background .15s; }
  .row-link:hover { background: var(--ty-surface-elevated); text-decoration: none; }
  .row-link:hover::before { background: var(--ty-color-primary); }
  .muted { color: var(--ty-text-faint); }
  .soft { color: var(--ty-text-soft); }
  .card:hover { transform: translateY(-1px); text-decoration: none; }
  .card { transition: transform .15s, border-color .15s; color: inherit; }
  .counter .value { color: var(--ty-color-primary); font-size: 2rem; font-weight: 700;
                    font-variant-numeric: tabular-nums; letter-spacing: -.02em; }
  .dl { font-variant-numeric: tabular-nums; font-family: var(--font-mono); }
  .empty { color: var(--ty-text-faint); font-style: italic; }
  .art { display: flex; align-items: center; justify-content: center; flex: none;
         color: rgba(255,255,255,.28); border-radius: 6px;
         border: 1px solid var(--ty-border-soft);
         position: relative; overflow: hidden; }
  .art img { position: absolute; inset: 0; width: 100%; height: 100%;
             object-fit: cover; }
  .art-sm { width: 44px; height: 44px; }
  .art-lg { width: 96px; height: 96px; border-radius: 10px;
            box-shadow: 0 6px 24px rgba(0,0,0,.35); }
  .rank { width: 1.6em; text-align: right; font-weight: 700; font-size: .85rem;
          color: var(--ty-text-faint); font-variant-numeric: tabular-nums; }
  .bar-row { display: grid; grid-template-columns: minmax(0,220px) 1fr auto;
             gap: 12px; align-items: center; padding: 6px 0; }
  .bar-track { background: var(--ty-surface-elevated); border-radius: 4px; height: 18px; }
  .bar-fill { height: 100%; border-radius: 4px; min-width: 2px;
              background: linear-gradient(90deg, var(--ty-color-primary), color-mix(in srgb, var(--ty-color-primary) 45%, transparent));
              transition: width .4s ease; }
  #toast { position: fixed; right: 20px; bottom: 20px; z-index: 500;
           display: flex; flex-direction: column; gap: 8px; align-items: flex-end; }
  .toast-msg { animation: toast 3s ease forwards; }
  @keyframes toast { 0% { opacity: 0; transform: translateY(8px); }
                     8% { opacity: 1; transform: none; }
                     80% { opacity: 1; } 100% { opacity: 0; } }
"""


def layout(title: str, user, content: str, stream: str | None = None,
           signals: dict | None = None) -> str:
    # Page-level signals live on <main>, OUTSIDE any SSE-patched fragment, so
    # live repaints never re-initialise them mid-edit.
    signals_attr = f" data-signals='{esc(_json.dumps(signals))}'" if signals else ""
    return f"""<!doctype html>
<html lang="en" class="dark" data-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{esc(title)} · datastar-music</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/tyrell-components@{TYRELL}/css/tyrell.css">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/tyrell-components@{TYRELL}/css/tyrell-brand.css">
  <script type="module" src="https://cdn.jsdelivr.net/npm/tyrell-components@{TYRELL}/dist/tyrell.js"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link rel="stylesheet"
    href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap">
  <script type="module" src="https://cdn.jsdelivr.net/gh/starfederation/datastar@{DATASTAR}/bundles/datastar.js"></script>
  <script src="https://cdn.tailwindcss.com"></script>
  <script>tailwind.config = {{ corePlugins: {{ preflight: false }} }}</script>
  <style>{_STYLE}</style>
  <script>
    (function () {{
      var reveal = function () {{ document.documentElement.classList.add("ty-ready"); }};
      if (window.customElements)
        Promise.all(["ty-button","ty-tag","ty-input"].map(function (n) {{
          return customElements.whenDefined(n); }})).then(reveal);
      setTimeout(reveal, 2000);
    }})();
  </script>
</head>
<body class="ty-canvas">
  <header class="topbar flex items-center px-6 py-2.5 gap-5 sticky top-0 z-[100]">
    <a class="brand flex items-center gap-2 font-semibold text-[0.95rem] tracking-[-0.01em]" href="/">
      {icon("disc", "sm")}<span>datastar-music</span>
    </a>
    <nav class="flex gap-0.5 flex-1 ml-1">
      {''.join(f'<a class="nav-link flex items-center gap-1.5 px-2.5 py-1 rounded-[5px] text-[0.84rem] font-medium" href="{h}">{icon(i,"xs")}{esc(t)}</a>' for h, i, t in _NAV)}
    </nav>
    <span class="live-badge"><span class="live-dot"></span>live · one SSE per page
      {tooltip("Watch events stream from Synthigy /data/events through the BFF to this page")}</span>
    <script>
      (function () {{ var p = location.pathname;
        document.querySelectorAll(".topbar nav a").forEach(function (a) {{
          var h = a.getAttribute("href");
          if (h && h !== "/" && (p === h || p.indexOf(h + "/") === 0)) a.setAttribute("aria-current","page");
        }}); }})();
    </script>
    <div class="flex items-center gap-2.5">
      {(f'<ty-tag size="sm" flavor="neutral">{icon("user","sm","start")}{esc(user.name)}</ty-tag>'
        '<form method="POST" action="/logout" style="display:inline">'
        f'<ty-button size="sm" flavor="neutral" type="submit">{icon("log-out","sm","start")}log out</ty-button></form>')
        if user else
        f'<a href="/login"><ty-button size="sm" flavor="primary">{icon("log-in","sm","start")}log in</ty-button></a>'}
    </div>
  </header>
  {f'<div id="page-stream" data-init="@get({stream!r})"></div>' if stream else ''}
  <main class="max-w-[1000px] mx-auto px-6 pt-9 pb-24"{signals_attr}>{content}</main>
  <div id="toast"></div>
  <footer class="border-t px-6 py-4 text-center text-[0.8em] muted"
          style="border-color:var(--ty-border)">
    Synthigy Python SDK demo · reads, writes, batches &amp; history over /data ·
    one SSE per page · BFF holds tokens · components by tyrell, reactivity by datastar
  </footer>
</body>
</html>"""


# ── landing ──────────────────────────────────────────────────────────────

def landing_page(user) -> str:
    cta = (f'<p class="m-0">You are logged in as <code>{esc(user.name)}</code>.</p>'
           if user else
           f'<a href="/login"><ty-button flavor="primary">{icon("log-in","sm","start")}'
           'Log in to start</ty-button></a>')
    cards = "".join(
        f'<a class="card surface block p-[22px] rounded-md" href="{h}">'
        f'<div class="mb-3" style="color:var(--ty-color-primary)">{icon(i,"lg")}</div>'
        f'<h2 class="m-0 mb-2 text-[1.05rem]">{esc(t)}</h2>'
        f'<p class="m-0 mb-3 text-[0.92em] soft">{esc(d)}</p>'
        f'<div class="flex flex-wrap gap-1.5">{"".join(sdk_chip(p) for p in prims)}</div></a>'
        for h, i, t, d, prims in [
            ("/albums", "disc", "Live album chart",
             "Top albums by plays, live. Search, filter to 1M+ hits, grow the "
             "window, and bump play counts — every tab repaints in place.",
             ["watch_entities()", "await stack()"]),
            ("/dashboard", "chart", "Live dashboard",
             "Counters + a live top-albums chart. First paint is ONE wire "
             "request (batch); updates ride a SQL-template watch.",
             ["await overview_async()  # @batch", "DashboardAsync.watch_stats()"]),
            ("/albums", "music", "Edit anything",
             "Add tracks, link and unlink genres, delete albums — nested "
             "writes, additive stack, surgical slice, soft delete.",
             ["await stack()", "await slice()", "await delete()"]),
        ])
    return layout("Home", user, f"""
      <section class="surface mt-6 mb-10 px-10 py-9 rounded-lg">
        <div class="flex gap-2 mb-[18px] flex-wrap">
          <ty-tag flavor="rating" size="xs">{icon("zap","xs","start")}Live data</ty-tag>
          <ty-tag flavor="neutral" size="xs">{icon("shield","xs","start")}No browser SDK</ty-tag>
          <ty-tag flavor="neutral" size="xs">{icon("server","xs","start")}BFF holds tokens</ty-tag>
          <ty-tag flavor="neutral" size="xs">{icon("layers","xs","start")}Typed ops from XSQL</ty-tag>
        </div>
        <h1 class="m-0 mb-3 text-[1.7rem] font-bold">The whole SDK, one music library.</h1>
        <p class="m-0 mb-4 max-w-[62ch] soft">A FastAPI BFF using the
          <code>synthigy</code> Python SDK against the <code>/data</code> endpoint:
          typed queries generated from <code>music.xsql</code>, live watches over
          SSE, nested writes, batches, ad-hoc filtered search and the audit
          history surface. The browser sees only HTML and SSE patches — no API
          keys, no JWT, no JSON wrangling.</p>
        {cta}
      </section>
      <section class="grid grid-cols-[repeat(auto-fit,minmax(280px,1fr))] gap-4 mb-10">{cards}</section>
      <section class="surface p-[22px] rounded-md">
        <h3 class="m-0 mb-3 text-[0.95rem] flex items-center gap-2">{icon("info","sm")}Under the hood</h3>
        <ul class="m-0 pl-5 soft">
          <li class="my-1.5">One <code>synthigy</code> client per BFF process; every call multiplexes identity with <code>acting_as=user.xid</code>.</li>
          <li class="my-1.5">One SSE from BFF → Synthigy <code>/data/events</code>, however many tabs are open.</li>
          <li class="my-1.5">Live updates are notify-then-refetch: writes don't repaint anything themselves — the watch does.</li>
          <li class="my-1.5">Typed ops (<code>MusicAlbum.list</code>, <code>Dashboard.watch_stats</code>, <code>stack_music_track</code>, …) are codegen'd from <code>xsql/music.xsql</code>.</li>
          <li class="my-1.5">Login is OIDC authorization-code with PKCE. Tokens stay server-side.</li>
          <li class="my-1.5">Look for the small <code class="sdkchip">code chips</code> — each names the SDK call behind that block.</li>
        </ul>
      </section>""")


# ── album list ───────────────────────────────────────────────────────────

def _artist_chips(artists) -> str:
    return "".join(
        f'<ty-tag size="xs" flavor="neutral">{esc(a.get("name"))}</ty-tag>'
        for a in (artists or [])[:3])


def album_row(a: dict, rank: int = 0) -> str:
    xid = a.get("xid")
    count = a.get("_count") or {}
    tracks = count.get("tracks", 0)
    plays = a.get("plays") or 0
    plays_title = f"{plays:,} plays"
    return f"""<li id="album-{esc(xid)}" class="row relative flex items-center pr-[14px]">
      <a class="row-link flex-1 min-w-0 grid grid-cols-[auto_auto_minmax(0,1fr)_auto] gap-4 items-center px-[18px] py-3"
         href="/albums/{esc(xid)}">
        <span class="rank">{rank if rank else ""}</span>
        {art(xid, cover=a.get("cover"))}
        <div class="min-w-0">
          <div class="truncate font-medium text-base mb-1">{esc(a.get("title") or "(untitled)")}</div>
          <div class="truncate text-[0.85rem] soft mb-1.5">{esc(a.get("blurb") or "")}</div>
          <div class="flex flex-wrap items-center gap-1.5">{_artist_chips(a.get("artists"))}</div>
        </div>
        <div class="inline-flex items-center gap-1.5 whitespace-nowrap">
          <ty-tag size="xs" flavor="neutral">
            {icon("music","xs","start")}<span class="dl">{tracks}</span>
            {tooltip(f"{tracks} track(s)")}</ty-tag>
          <ty-tag size="xs" flavor="rating">
            {icon("play","xs","start")}<span class="dl font-semibold">{esc(fmt_plays(plays))}</span>
            {tooltip(plays_title)}</ty-tag>
        </div>
      </a>
      <ty-button action pill size="sm" appearance="ghost" flavor="primary"
        data-on:click="@post('/albums/{esc(xid)}/play')">
        {icon("play","sm")}{tooltip("Play — +1 play via stack_music_album()")}</ty-button>
    </li>"""


# The one search/filter/window/sort refetch — every control funnels through it.
_REFRESH = ("@get('/albums-search?q='+encodeURIComponent($q)"
            "+'&limit='+$limit+'&hits='+$hits+'&sort='+$sort)")

_SORTS = [("plays", "Most played"), ("az", "Title A→Z"),
          ("za", "Title Z→A"), ("newest", "Recently added")]


def albums_page(user, initial: list[dict], term: str, hits: bool,
                sort: str = "plays") -> str:
    shown = len(initial)
    sort_options = "".join(
        f'<ty-option value="{v}"{" selected" if v == sort else ""}>{esc(label)}</ty-option>'
        for v, label in _SORTS)
    content = f"""
      <div class="flex items-center justify-between mb-4 gap-4">
        <h1 class="m-0 text-[1.4rem] font-bold flex items-center gap-2">{icon("disc","md")}Albums</h1>
        <span class="flex items-center gap-2">
          {sdk_chip('watch_entities() → await MusicAlbumAsync.list(?sort)',
                    "Entity poke channel + refetch with current signals — "
                    "the stream itself never changes")}
          <span id="albums-meta" class="muted text-[0.85rem] dl">{search_meta(term, shown)}</span>
          <ty-button size="sm" flavor="neutral" data-on:click="$_adding=!$_adding">
            {icon("plus","sm","start")}Add album
            {tooltip("New album via stack — nested artist write, deterministic artist+title identity")}</ty-button>
        </span>
      </div>
      <div data-show="$_adding" class="surface rounded-lg p-4 mb-4" style="display:none">
        <div class="flex items-center justify-between mb-3">
          <span class="muted text-[0.72rem] uppercase tracking-wider">New album</span>
          {sdk_chip("await aclient().stack(MusicAlbumInput) + nested artist")}
        </div>
        <div class="flex gap-2 items-center flex-wrap">
          <ty-input size="sm" placeholder="Album title *" class="flex-1 min-w-[180px]"
            data-bind="album_title"></ty-input>
          <ty-input size="sm" placeholder="Artist *" class="flex-1 min-w-[140px]"
            data-bind="album_artist"></ty-input>
          <ty-input size="sm" placeholder="One-line blurb" class="flex-[2] min-w-[200px]"
            data-bind="album_blurb"
            data-on:keydown="evt.key==='Enter' && @post('/albums/new')"></ty-input>
          <ty-button size="sm" flavor="primary" data-on:click="@post('/albums/new')">
            {icon("plus","sm","start")}Create</ty-button>
          <ty-button size="sm" flavor="neutral" data-on:click="$_adding=false">Cancel</ty-button>
        </div>
        <p class="m-0 mt-2 text-[0.78rem] muted">Identity is the deterministic
          artist+title xid — creating the same album twice upserts instead of
          duplicating. The artist is a nested write: created if new, linked if known.
          Every open tab's list, search and dashboard repaint from the same watch events.</p>
      </div>
      <div class="flex gap-2 mb-4 items-center">
        <ty-input placeholder="Filter by title…" size="md" class="flex-1 block"
          data-bind="q" value="{esc(term)}"
          data-on:input__debounce.300ms="$limit={PER_PAGE};{_REFRESH}"></ty-input>
        <ty-select size="sm" class="w-[170px]"
          data-on:change="$sort=evt.detail.value;$limit={PER_PAGE};{_REFRESH}">
          {sort_options}
        </ty-select>
        <label class="flex items-center gap-2 text-[0.88rem] soft whitespace-nowrap cursor-pointer">
          <ty-switch data-attr:checked="$hits"
            data-on:change="$hits=evt.detail.checked;$limit={PER_PAGE};{_REFRESH}"></ty-switch>
          {icon("flame","sm")}1M+ plays
          {tooltip("Only albums with 1M+ plays — a gt() filter on plays")}
        </label>
      </div>
      <ul id="albums" class="surface rounded-lg overflow-hidden m-0 p-0 list-none">{albums_list_html(initial, term)}</ul>
      <div id="poke"></div>
      <div class="mt-4 text-center">
        <ty-button size="sm" flavor="neutral"
          data-on:click="$limit=$limit+{PER_PAGE};{_REFRESH}">
          {icon("download","sm","start")}Show more
          {tooltip("Grow the query window — ?limit param; the live watch follows")}</ty-button>
      </div>"""
    # ONE fixed stream per page — a poke channel with no query window. Live
    # events patch #poke with a node that re-fires the refresh using the
    # browser's CURRENT signals, so sort/search/toggle never re-point the
    # stream (see poke_html for why re-pointing leaks connections).
    return layout("Albums", user, content, stream="/stream/albums",
                  signals={"q": term, "limit": PER_PAGE, "hits": hits,
                           "sort": sort, "_adding": False, "album_title": "",
                           "album_artist": "", "album_blurb": ""})


def search_meta(term: str, shown: int) -> str:
    # `shown` is the current window, not a true match count — the list query
    # has no COUNT companion. Word it as a window so it doesn't lie.
    if term:
        return f'showing top {shown} for "{esc(term)}"'
    return f"showing top {shown}"


def albums_list_html(rows: list[dict], term: str) -> str:
    return "".join(album_row(a, i + 1) for i, a in enumerate(rows)) or \
        f'<li class="empty px-[22px] py-4">No albums match "{esc(term)}".</li>'


# ── album detail ─────────────────────────────────────────────────────────

def _track_row(t: dict) -> str:
    txid = t.get("xid")
    explicit = t.get("explicit")
    title = esc(t.get("title") or "?")
    when = esc((t.get("release_on") or "")[:10])
    length = t.get("length")
    # Quiet "E" square, the streaming-app convention — not a red warning.
    badge = (f'<ty-tag size="xs" flavor="neutral">E{tooltip("explicit lyrics")}</ty-tag>'
             if explicit else "")
    lentag = f'<span class="muted text-[0.8rem] dl">{esc(length)}</span>' if length else ""
    return (f'<div class="row flex items-center gap-3 px-4 py-2.5">'
            f'<ty-tag size="xs" flavor="neutral">{icon("music","xs","start")}{title}</ty-tag>'
            f'<span class="muted text-[0.82rem] dl">{when}</span>'
            f'<span class="ml-auto flex items-center gap-2">{lentag}{badge}'
            f'<ty-button action pill size="xs" appearance="ghost" flavor="danger"'
            f' data-on:click="@post(\'/tracks/{esc(txid)}/delete\')">{icon("trash","xs")}'
            f'{tooltip("Remove — delete_music_track() (soft delete)")}</ty-button>'
            f'</span></div>')


def _genre_chip(axid: str | None, g: dict) -> str:
    gxid = g.get("xid")
    return (f'<ty-tag size="xs" flavor="neutral" dismissible'
            f' data-on:dismiss="@post(\'/albums/{esc(axid)}/genres/{esc(gxid)}/remove\')">'
            f'{esc(g.get("label"))}'
            f'{tooltip("Unlink — sync() replaces the link-set minus this one")}</ty-tag>')


def album_detail_card(a: dict) -> str:
    xid = a.get("xid")
    tracks = a.get("tracks") or []
    artists = a.get("artists") or []
    genres = a.get("genres") or []
    link = a.get("link")
    link_html = (f'<a href="{esc(link)}" target="_blank" rel="noopener" '
                 f'class="inline-flex items-center gap-1 text-[0.85rem]">'
                 f'{icon("external","xs")}listen</a>') if link else ""
    track_html = "".join(_track_row(t) for t in tracks) or \
        '<div class="empty px-4 py-3">No tracks.</div>'
    artist_html = "".join(
        f'<div class="flex items-center gap-2 px-4 py-2 row">'
        f'{icon("user","sm")}<span class="font-medium">{esc(ar.get("name"))}</span>'
        f'<span class="muted text-[0.82rem] ml-auto">{esc(ar.get("country") or "")}</span></div>'
        for ar in artists) or '<div class="empty px-4 py-3">No artists.</div>'
    genre_html = "".join(_genre_chip(xid, g) for g in genres) or \
        '<span class="empty">No genres.</span>'
    plays = a.get("plays") or 0
    plays_title = f"{plays:,} total plays"
    return f"""<div id="album-{esc(xid)}" class="surface rounded-lg overflow-hidden">
      <div class="px-6 py-5" style="border-bottom:1px solid var(--ty-border)">
        <div class="flex items-start gap-5">
          {art(xid, "art-lg", "xl", cover=a.get("cover"))}
          <div class="flex-1 min-w-0">
            <h1 class="m-0 text-[1.5rem] font-bold">{esc(a.get("title"))}</h1>
            <p class="m-0 mt-1 soft">{esc(a.get("blurb") or "")}</p>
            <div class="mt-2">{link_html}</div>
          </div>
          <div class="flex flex-col items-end gap-2">
            <ty-tag flavor="rating" size="md">
              {icon("play","sm","start")}<span class="dl font-bold">{esc(fmt_plays(plays))}</span>
              {tooltip(plays_title)}</ty-tag>
            <ty-button size="sm" flavor="primary"
              data-on:click="@post('/albums/{esc(xid)}/play')">
              {icon("play","sm","start")}Play
              {tooltip("+1 play — stack_music_album(); watches repaint every tab")}</ty-button>
          </div>
        </div>
      </div>
      <div class="grid grid-cols-[1fr] md:grid-cols-[2fr_1fr]">
        <div style="border-right:1px solid var(--ty-border)">
          <div class="px-4 pt-3 pb-1 muted text-[0.72rem] uppercase tracking-wider flex items-center justify-between">
            <span>Tracks</span>{sdk_chip("await aclient().stack(MusicTrackInput)")}</div>
          {track_html}
          <div class="px-4 py-3" style="border-top:1px solid var(--ty-border-soft)">
            <div class="flex items-center gap-2">
              <ty-input size="sm" placeholder="Track title" class="flex-1" data-bind="track_title"></ty-input>
              <ty-input size="sm" placeholder="3:45" class="w-[70px]" data-bind="track_length"></ty-input>
              <ty-button size="sm" flavor="neutral"
                data-on:click="@post('/albums/{esc(xid)}/tracks')">
                {icon("plus","sm","start")}Add
                {tooltip("Nested write: track links to this album via album: xid")}</ty-button>
            </div>
            <label class="flex items-center gap-2 mt-2 text-[0.8rem] muted cursor-pointer">
              <ty-switch size="sm" data-attr:checked="$track_explicit"
                data-on:change="$track_explicit=evt.detail.checked"></ty-switch>
              explicit lyrics</label>
          </div>
        </div>
        <div>
          <div class="px-4 pt-3 pb-1 muted text-[0.72rem] uppercase tracking-wider">Artists</div>
          {artist_html}
          <div class="px-4 pt-3 pb-1 muted text-[0.72rem] uppercase tracking-wider flex items-center justify-between">
            <span>Genres</span>{sdk_chip("await stack() / sync() / slice()")}</div>
          <div class="px-4 py-2 flex flex-wrap gap-1.5 items-center">{genre_html}</div>
          <div class="flex items-center gap-2 px-4 py-3">
            <ty-input size="sm" placeholder="Add genre…" class="flex-1" data-bind="genre_label"
              data-on:keydown="evt.key==='Enter' && @post('/albums/{esc(xid)}/genres')"></ty-input>
            <ty-button action pill size="sm" flavor="neutral"
              data-on:click="@post('/albums/{esc(xid)}/genres')">{icon("plus","sm")}
              {tooltip("stack_music_album() with a deterministic-xid genre — additive, idempotent")}</ty-button>
            <ty-button size="sm" flavor="neutral"
              data-on:click="confirm('Unlink ALL genres? (slice cuts the whole link-set)') && @post('/albums/{esc(xid)}/genres/clear')">
              {icon("x","sm","start")}clear
              {tooltip("synthigy.slice() — cuts the whole genres link-set in one op")}</ty-button>
          </div>
        </div>
      </div>
    </div>"""


def history_html(events) -> str:
    """Audit timeline via synthigy.history().events(record_xid=…).
    Rendered once at page load; empty string when no audit provider."""
    if not events:
        return ""
    rows = []
    for ev in events[:10]:
        if not isinstance(ev, dict):
            continue
        ts = ev.get("ts") or ev.get("at") or ev.get("time") or ""
        op = ev.get("op") or ev.get("action") or ev.get("event") or "change"
        who = ev.get("user") or ev.get("by") or ev.get("actor") or ""
        if isinstance(who, dict):
            who = who.get("name") or who.get("xid") or ""
        rows.append(
            f'<div class="row flex items-center gap-3 px-4 py-2">'
            f'{icon("clock","xs")}'
            f'<span class="dl muted text-[0.8rem]">{esc(str(ts)[:19].replace("T", " "))}</span>'
            f'<ty-tag size="sm" flavor="neutral">{esc(op)}</ty-tag>'
            f'<span class="muted text-[0.82rem] ml-auto">{esc(who)}</span></div>')
    if not rows:
        return ""
    return f"""<section class="surface rounded-lg overflow-hidden mt-6">
      <div class="px-4 pt-3 pb-1 muted text-[0.72rem] uppercase tracking-wider flex items-center justify-between">
        <span>Change history</span>{sdk_chip("await aclient().history.events(record_xid=…)")}</div>
      {''.join(rows)}
    </section>"""


def album_detail_page(user, album: dict, history: str = "") -> str:
    xid = album.get("xid")
    content = f"""
      <div class="flex items-center justify-between mb-4">
        <a href="/albums" class="inline-flex items-center gap-1 muted text-[0.85rem]">
          {icon("arrow-left","xs")}all albums</a>
        <ty-button size="sm" flavor="danger"
          onclick="document.getElementById('delete-modal').show()">
          {icon("trash","sm","start")}Delete album
          {tooltip("delete_music_album() — soft delete; the list watch drops it live")}</ty-button>
      </div>
      <ty-modal id="delete-modal">
        <div class="surface p-6 rounded-lg max-w-md">
          <h3 class="m-0 mb-2 text-[1.05rem] font-semibold flex items-center gap-2">
            {icon("trash","sm")}Delete “{esc(album.get("title"))}”?</h3>
          <p class="m-0 mb-4 soft text-[0.9rem]">Soft delete via
            <code>delete_music_album()</code> — every open list drops it live.</p>
          <div class="flex justify-end gap-2">
            <ty-button size="sm" flavor="neutral"
              onclick="document.getElementById('delete-modal').hide()">Cancel</ty-button>
            <form method="POST" action="/albums/{esc(xid)}/delete" style="display:inline">
              <ty-button size="sm" flavor="danger" type="submit">
                {icon("trash","sm","start")}Delete</ty-button>
            </form>
          </div>
        </div>
      </ty-modal>
      {album_detail_card(album)}
      {history}"""
    return layout(album.get("title") or "Album", user, content,
                  stream=f"/stream/albums/{esc(xid)}",
                  signals={"track_title": "", "track_length": "",
                           "track_explicit": False, "genre_label": ""})


# ── dashboard ────────────────────────────────────────────────────────────

COUNTERS = [
    ("albums", "disc", "Albums"),
    ("tracks", "music", "Tracks"),
    ("plays", "play", "Plays"),
    ("artists", "users", "Artists"),
]


def counter_text(key: str, val) -> str:
    if key == "plays":
        return fmt_plays(val)  # this tile is the one that overflows at 10+ digits
    return f"{val:,}" if isinstance(val, int) else ("—" if val is None else str(val))


def top_chart_html(rows: list[dict]) -> str:
    if not rows:
        return '<div class="empty px-2 py-3">No albums yet.</div>'
    top = max((r.get("plays") or 0) for r in rows) or 1
    bars = "".join(
        f'<div class="bar-row">'
        f'<a class="truncate text-[0.88rem]" href="/albums/{esc(r.get("xid"))}">{esc(r.get("title"))}</a>'
        f'<div class="bar-track"><div class="bar-fill" style="width:{max(1, round(100 * (r.get("plays") or 0) / top))}%"></div></div>'
        f'<span class="dl muted text-[0.82rem] w-[52px] text-right">{esc(fmt_plays(r.get("plays")))}</span>'
        f'</div>'
        for r in rows)
    return bars


def _bclub_html(rows: list[dict]) -> str:
    if not rows:
        return '<div class="empty">Nobody yet — go press play a billion times.</div>'
    return "".join(
        f'<div class="row flex items-center gap-3 px-1 py-2">'
        f'{art(r.get("xid"), "art-sm", "sm", cover=r.get("cover"))}'
        f'<a class="truncate flex-1 text-[0.9rem]" href="/albums/{esc(r.get("xid"))}">{esc(r.get("title"))}</a>'
        f'<span class="dl font-semibold" style="color:var(--ty-color-primary)">{esc(fmt_plays(r.get("plays")))}</span></div>'
        for r in rows)


def dashboard_page(user, stats: dict, top: list[dict], bclub: list[dict]) -> str:
    tiles = "".join(
        f'<div class="counter surface rounded-lg p-6">'
        f'<div class="flex items-center gap-2 muted text-[0.8rem] uppercase tracking-wider mb-2">'
        f'{icon(i,"sm")}{esc(label)}</div>'
        f'<div id="counter-{key}-val" class="value">{counter_text(key, stats.get("total_" + key))}</div></div>'
        for key, i, label in COUNTERS)
    content = f"""
      <div class="flex items-center justify-between mb-1">
        <h1 class="m-0 text-[1.4rem] font-bold flex items-center gap-2">{icon("chart","md")}Dashboard</h1>
        {sdk_chip("await overview_async()  # @batch: list + stats, ONE request")}
      </div>
      <p class="muted mb-6 text-[0.9rem]">First paint is one batched wire call;
        live updates ride <code>Dashboard.watch_stats()</code> — a SQL-template
        watch over three entities.</p>
      <div class="grid grid-cols-[repeat(auto-fit,minmax(180px,1fr))] gap-4 mb-8">{tiles}</div>
      <div class="grid grid-cols-1 md:grid-cols-[3fr_2fr] gap-4">
        <section class="surface rounded-lg p-5">
          <div class="flex items-center justify-between mb-3">
            <h3 class="m-0 text-[0.95rem] flex items-center gap-2">{icon("chart","sm")}Most played</h3>
            {sdk_chip("DashboardAsync.watch_stats() → await MusicAlbumAsync.list()")}
          </div>
          <div id="top-chart">{top_chart_html(top)}</div>
        </section>
        <section class="surface rounded-lg p-5">
          <div class="flex items-center justify-between mb-3">
            <h3 class="m-0 text-[0.95rem] flex items-center gap-2">{icon("flame","sm")}Billion club</h3>
            {sdk_chip('await aclient().search("music_album", {"plays": gt(1e9)})')}
          </div>
          {_bclub_html(bclub)}
        </section>
      </div>"""
    return layout("Dashboard", user, content, stream="/stream/dashboard")
