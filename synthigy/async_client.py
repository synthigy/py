"""The Synthigy engine — async-native client. `from synthigy import AsyncClient`.

ONE engine: every feature of the SDK runs on this asyncio core. The
blocking `synthigy.Client` is a thin facade over this class driving one
background event-loop thread (see facade.py) — an idle watch is a
suspended coroutine on ONE shared SSE connection, not a parked OS thread.
Measured: 200 concurrent watches cost 2 threads here vs 202 on the old
thread-per-watch core this replaced. Stdlib-only, like the rest of the SDK.

Feature surface (parity with sdk/go watch.go and sdk/js watch.js unless
noted):
  - transport: hand-rolled async HTTP/1.1 keep-alive pool (chunked +
    HTTP/1.0 close semantics, stale-keepalive retry-once, Semaphore-gated
    so fan-out can't become a connection storm), per-call timeout= on
    every /data verb, on_op observability callback
  - auth: client-credentials (scope=) or static token, 401
    clear+retry-once on BOTH the request and SSE legs, acting_as identity
    multiplexing (client default + per-call override), key_format
    (client default + per-call), token(audience=) accessor
  - ops: search/get/query(XSQL)/sql_template, sync/stack/slice/delete/
    purge, exec_ (batch), search_tree/get_tree (composed), deployed/
    runtime model, schema, lint, history (get_at/events/diff/timeline/
    since)
  - streaming: listen() (raw SSE envelopes, infinite reconnect with
    Last-Event-ID resume), observe() (one-call live primitive with
    re-register-per-session + optional /history backfill across gaps)
  - subscriptions: subscribe/unsubscribe/subscribe_model/set_subscriptions/
    clear_subscriptions/subscriptions() — records-only full set-replace
    wire (advanced; raw calls clobber a live watch multiplexer's union)
  - watch layer: one mux = one SSE + one consolidated subscription/set
    union POST (signature-deduped, post-open flush, reconnect re-flush +
    connection/resumed sentinel); schema tracking (runtime-model deploys
    refresh the resolver and fan out schema/changed); AsyncWatchHandle
    (raw events, live add/remove/set_interest, mute_request, coalesce/
    lossless/sliding backpressure); AsyncQueryWatch (DERIVED
    query/added|changed|removed events with before/after/changed +
    provenance, coalesced refetch, automatic interest re-sync,
    relation-xid interest aid, entity-touch fallback); AsyncSqlTemplateWatch
    (result/changed); AsyncSchemaWatch (schema/changed); AsyncEntityWatch
    (raw entity-touch pokes); keep_alive pins the SSE session across
    watch churn
  - lifecycle: `async with` everywhere; close()/unregister are await-free
    so cleanup survives an already-cancelled scope (asyncio law: cleanup
    that must run on cancellation cannot contain awaits); fatal auth
    errors end every stream via sentinel + StopAsyncIteration instead of
    going silently deaf
"""

from __future__ import annotations

import asyncio
import json
import os
import ssl
import time
import urllib.parse

from .backfill import fold_history_events
from .compose import compose_forest, compose_tree
from .errors import SynthigyError, error_from_server
from .events import (
    REFRESH_COALESCE, _collect_all_xids, _diff_row, _event_matches_descriptor,
    _row_key, _rows_equal, _union_changed, descriptor_key, kebab, matches,
    normalize_descriptor, normalize_interest, shape_event,
)
from .selection import normalize_selection
from .util import generate_request_id, now_iso, xsql_document

_INITIAL_BACKOFF = 1.0
_MAX_BACKOFF = 30.0


# ── async transport ─────────────────────────────────────────────────────

class _AsyncConn:
    __slots__ = ("reader", "writer", "host")

    def __init__(self, reader, writer, host):
        self.reader, self.writer, self.host = reader, writer, host


class _AsyncPool:
    """Async keep-alive HTTP/1.1 pool. One idle-connection list per
    (scheme, host, port); a stale keep-alive connection (peer closed
    between requests) is retried ONCE on a fresh one."""

    # An async core makes 500-wide fan-out one gather() away — cap
    # concurrent requests so a traffic spike can't become a connection
    # storm on the Synthigy server.
    MAX_CONCURRENT = 32

    def __init__(self, timeout=None):
        self._idle: dict[tuple, list] = {}
        self._lock = asyncio.Lock()
        self._gate = asyncio.Semaphore(self.MAX_CONCURRENT)
        self._timeout = timeout
        self._closed = False

    async def _connect(self, scheme, host, port):
        ssl_ctx = ssl.create_default_context() if scheme == "https" else None
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=ssl_ctx),
            timeout=self._timeout)
        return _AsyncConn(reader, writer, host)

    async def request(self, method, url, body=None, headers=None):
        """-> (status, headers-dict, body-bytes). Raises SynthigyError on
        transport failure (network error, not exhausted-retries)."""
        async with self._gate:
            return await self._request(method, url, body, headers)

    async def _request(self, method, url, body=None, headers=None):
        u = urllib.parse.urlsplit(url)
        scheme = u.scheme or "http"
        port = u.port or (443 if scheme == "https" else 80)
        key = (scheme, u.hostname, port)
        path = (u.path or "/") + (f"?{u.query}" if u.query else "")
        body = body or b""

        for attempt in (0, 1):
            async with self._lock:
                idle = self._idle.get(key)
                conn = idle.pop() if idle else None
            fresh = conn is None
            try:
                if fresh:
                    conn = await self._connect(scheme, u.hostname, port)
                status, resp_headers, data, will_close = await asyncio.wait_for(
                    self._exchange(conn, method, path, body, headers or {}),
                    timeout=self._timeout)
            except (OSError, asyncio.TimeoutError, ConnectionError) as e:
                if conn is not None:
                    conn.writer.close()
                if fresh or attempt:
                    raise SynthigyError(f"network error: {e}",
                                        "NETWORK_ERROR") from e
                continue  # stale keep-alive — retry once on a fresh conn
            if will_close or self._closed:
                conn.writer.close()
            else:
                async with self._lock:
                    self._idle.setdefault(key, []).append(conn)
            return status, resp_headers, data
        raise SynthigyError("network error: exhausted retries", "NETWORK_ERROR")

    @staticmethod
    async def _exchange(conn, method, path, body, headers):
        h = dict(headers)
        h.setdefault("Host", conn.host)
        h.setdefault("Connection", "keep-alive")
        if body:
            h.setdefault("Content-Length", str(len(body)))
        req_lines = [f"{method} {path} HTTP/1.1"] + [f"{k}: {v}" for k, v in h.items()]
        conn.writer.write(("\r\n".join(req_lines) + "\r\n\r\n").encode() + body)
        await conn.writer.drain()

        status_line = await conn.reader.readline()
        if not status_line:
            raise ConnectionError("connection closed by peer")
        parts = status_line.decode().split(None, 2)
        status = int(parts[1])

        resp_headers = {}
        while True:
            line = await conn.reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break
            k, _, v = line.decode().partition(":")
            resp_headers[k.strip().lower()] = v.strip()

        # HTTP/1.0 closes after the response unless keep-alive is explicit;
        # HTTP/1.1 keeps alive unless close is explicit. (http.client gave
        # the old sync pool this via will_close — hand-rolled, we must
        # match, or a dead connection gets pooled and the next request
        # eats a ConnectionError.)
        conn_hdr = resp_headers.get("connection", "").lower()
        if parts[0] == "HTTP/1.0":
            will_close = conn_hdr != "keep-alive"
        else:
            will_close = conn_hdr == "close"
        if "content-length" in resp_headers:
            n = int(resp_headers["content-length"])
            data = await conn.reader.readexactly(n) if n else b""
        elif resp_headers.get("transfer-encoding", "").lower() == "chunked":
            data = await _read_chunked(conn.reader)
        else:
            data = await conn.reader.read(-1)
            will_close = True
        return status, resp_headers, data, will_close

    async def close(self):
        async with self._lock:
            self._closed = True
            idle, self._idle = self._idle, {}
        for conns in idle.values():
            for c in conns:
                c.writer.close()


async def _read_chunked(reader):
    out = bytearray()
    while True:
        size_line = await reader.readline()
        size = int(size_line.split(b";")[0].strip(), 16)
        if size == 0:
            await reader.readline()
            break
        out += await reader.readexactly(size)
        await reader.readline()
    return bytes(out)


async def _open_sse(endpoint, token, last_event_id=None, timeout=None):
    """Open the SSE GET, parse status+headers, leave the reader positioned
    at the frame stream. Dedicated connection (not pooled) — SSE blocks
    by design and never returns to the pool."""
    u = urllib.parse.urlsplit(endpoint + "/data/events")
    scheme = u.scheme or "http"
    port = u.port or (443 if scheme == "https" else 80)
    ssl_ctx = ssl.create_default_context() if scheme == "https" else None
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(u.hostname, port, ssl=ssl_ctx), timeout=timeout)

    headers = {"Accept": "text/event-stream", "Host": u.hostname,
               "Connection": "keep-alive",
               "X-Request-Id": generate_request_id()}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    path = (u.path or "/") + (f"?{u.query}" if u.query else "")
    req_lines = [f"GET {path} HTTP/1.1"] + [f"{k}: {v}" for k, v in headers.items()]
    writer.write(("\r\n".join(req_lines) + "\r\n\r\n").encode())
    await writer.drain()

    status_line = await reader.readline()
    if not status_line:
        writer.close()
        raise SynthigyError("network error: connection closed", "NETWORK_ERROR")
    status = int(status_line.decode().split(None, 2)[1])
    resp_headers = {}
    while True:
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        k, _, v = line.decode().partition(":")
        resp_headers[k.strip().lower()] = v.strip()

    if status == 401:
        writer.close()
        raise SynthigyError("Unauthorized", "UNAUTHORIZED", status=401)
    if status == 403:
        writer.close()
        raise SynthigyError("Forbidden", "FORBIDDEN", status=403)
    if not 200 <= status < 300:
        writer.close()
        raise SynthigyError(f"SSE connect failed ({status})", "HTTP_ERROR",
                            status=status)
    # Real servers stream SSE with Transfer-Encoding: chunked (the correct
    # mechanism for an unbounded-length response) — the stdlib stub used by
    # the unit tests never chunks, which is why this only surfaced against a
    # real server: unwrapped, chunk-size lines get parsed as SSE text and
    # every frame silently fails its JSON decode (dropped, no error raised).
    body_reader = reader
    if resp_headers.get("transfer-encoding", "").lower() == "chunked":
        body_reader = _ChunkedReader(reader)
    return body_reader, writer


class _ChunkedReader:
    """readline()-compatible wrapper over an asyncio.StreamReader carrying
    a Transfer-Encoding: chunked body — strips chunk-size/CRLF framing so
    callers see only the decoded content bytes."""

    def __init__(self, reader):
        self._reader = reader
        self._buf = bytearray()
        self._chunk_remaining = 0
        self._eof = False

    async def _fill(self):
        if self._eof:
            return False
        if self._chunk_remaining == 0:
            size_line = await self._reader.readline()
            if not size_line:
                self._eof = True
                return False
            size = int(size_line.split(b";")[0].strip(), 16)
            if size == 0:
                await self._reader.readline()  # trailer + final CRLF
                self._eof = True
                return False
            self._chunk_remaining = size
        data = await self._reader.read(min(self._chunk_remaining, 65536))
        if not data:
            self._eof = True
            return False
        self._chunk_remaining -= len(data)
        self._buf += data
        if self._chunk_remaining == 0:
            await self._reader.readline()  # chunk-terminating CRLF
        return True

    async def readline(self):
        while b"\n" not in self._buf:
            if not await self._fill():
                break
        idx = self._buf.find(b"\n")
        if idx == -1:
            line, self._buf = bytes(self._buf), bytearray()
            return line
        line = bytes(self._buf[:idx + 1])
        del self._buf[:idx + 1]
        return line


async def _sse_frames(reader):
    """Async generator over one SSE connection's frames — standard
    event:/data:/id: block parser."""
    event_type, data_lines, event_id = "message", [], None
    while True:
        raw = await reader.readline()
        if not raw:
            return
        line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        if line.startswith("event:"):
            event_type = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].strip())
        elif line.startswith("id:"):
            event_id = line[3:].strip()
        elif line == "":
            if data_lines:
                try:
                    payload = json.loads("\n".join(data_lines))
                except ValueError:
                    payload = None
                if isinstance(payload, dict):
                    yield {"_sse_event": event_type, "_sse_id": event_id, **payload}
            event_type, data_lines, event_id = "message", [], None


# ── schema resolver ─────────────────────────────────────────────────────

class AsyncSchemaResolver:
    """Schema resolver — entity name -> entity-xid, relation xids per
    entity, attr-xid -> attribute name. Lazy cache over client.schema()
    (plain JSON, IAM-projected); refresh() re-pulls on runtime-model
    deploy events. One per client, owned by the watch multiplexer.
    Accessors are plain sync dict reads (single event loop, no locks)."""

    def __init__(self, client):
        self._client = client
        self._loaded = False
        self._load_lock = asyncio.Lock()   # coalesces concurrent fetches
        self._attr_xid_to_name = {}
        self._table_xid_to_name = {}
        self._name_to_table_xid = {}
        self._rels_by_entity = {}

    async def ensure_loaded(self):
        """Load once; concurrent callers coalesce on the fetch lock.
        Failure does not poison the cache — the next call retries."""
        async with self._load_lock:
            if self._loaded:
                return
            await self._fetch()

    async def refresh(self):
        """Force a re-pull (runtime-model deploy landed)."""
        async with self._load_lock:
            await self._fetch()

    async def _fetch(self):
        schema = await self._client.schema()
        attr, table, name_to_table, rels = {}, {}, {}, {}
        entities = (schema or {}).get("entities") or {}
        for name, entity in entities.items():
            if not isinstance(entity, dict):
                continue
            xid = entity.get("xid")
            if xid:
                table[str(xid)] = name
                name_to_table[name] = str(xid)
            xids = entity.get("xids") or {}
            for attr_name, attr_xid in (xids.get("attributes") or {}).items():
                if attr_xid:
                    attr[str(attr_xid)] = attr_name
            rel_xids = [str(x) for x in (xids.get("relations") or {}).values()
                        if x]
            if xid and rel_xids:
                existing = rels.get(str(xid), [])
                merged = list(existing)
                seen = set(existing)
                for x in rel_xids:
                    if x not in seen:
                        seen.add(x)
                        merged.append(x)
                rels[str(xid)] = merged
        self._attr_xid_to_name = attr
        self._table_xid_to_name = table
        self._name_to_table_xid = name_to_table
        self._rels_by_entity = rels
        self._loaded = True

    def attr_name(self, xid):
        """Attribute name for an attr xid; the xid itself when unmapped."""
        return self._attr_xid_to_name.get(str(xid), str(xid))

    def entity_name(self, xid):
        return self._table_xid_to_name.get(str(xid))

    def entity_xid_by_name(self, name):
        """Kebab-case entity name -> entity xid; None when unknown."""
        return self._name_to_table_xid.get(kebab(name))

    def relations_for_entity(self, entity_xid):
        """All relation xids touching entity_xid (either side)."""
        if not entity_xid:
            return []
        return list(self._rels_by_entity.get(str(entity_xid), []))

    def is_loaded(self):
        return self._loaded


# ── async watch layer ───────────────────────────────────────────────────

class _AsyncCoalesceBuffer:
    """Coalescing event buffer — one per watch. Mirrors sdk/js watch.js
    CoalesceBuffer / sdk/go coalesce.go:
      - "coalesce" (default): pending record/* events on the SAME record
        collapse — delete supersedes, updates merge (earliest before,
        latest after, union of changed); overflow drops the oldest.
      - "lossless": append-only; overflow emits one {"type": "paused"}
        sentinel then drops until drained.
      - "sliding": overflow drops the oldest.
    push() is sync (called from the mux dispatch); pop() awaits. close()
    ends the stream: pop() returns None once drained."""

    def __init__(self, mode="coalesce", size=100):
        self._mode = mode
        self._size = size
        self._queue: list = []
        self._by_record: dict = {}
        self._event = asyncio.Event()
        self._closed = False
        self._paused = False

    def _reindex(self):
        self._by_record = {ev.get("record"): i
                           for i, ev in enumerate(self._queue)
                           if (ev.get("type") or "").startswith("record/")}

    def push(self, ev):
        if self._closed:
            return
        etype = ev.get("type") or ""
        if self._mode == "coalesce" and etype.startswith("record/"):
            idx = self._by_record.get(ev.get("record"))
            if idx is not None:
                prev = self._queue[idx]
                if etype == "record/delete":
                    self._queue[idx] = ev            # delete supersedes
                elif prev.get("type") == "record/delete":
                    pass                              # delete already wins
                else:
                    merged = dict(ev)
                    before = (prev.get("before")
                              if prev.get("before") is not None
                              else ev.get("before"))
                    if before is not None:
                        merged["before"] = before
                    changed = _union_changed(prev.get("changed"),
                                             ev.get("changed"))
                    if changed is not None:
                        merged["changed"] = changed
                    self._queue[idx] = merged
                self._event.set()
                return
        if len(self._queue) >= self._size:
            if self._mode == "lossless":
                if not self._paused:
                    self._paused = True
                    self._queue.append({"type": "paused"})
                    self._event.set()
                return                                # paused; drop silently
            self._queue.pop(0)                        # coalesce/sliding
            self._reindex()
        if etype.startswith("record/"):
            self._by_record[ev.get("record")] = len(self._queue)
        self._queue.append(ev)
        self._paused = False
        self._event.set()

    async def pop(self):
        """Next event; None once closed AND drained."""
        while True:
            if self._queue:
                ev = self._queue.pop(0)
                if self._mode == "coalesce":
                    self._reindex()
                return ev
            if self._closed:
                return None
            self._event.clear()
            await self._event.wait()

    def close(self):
        self._closed = True
        self._event.set()


class AsyncWatchHandle:
    """Raw live subscription — parity with Go's WatchHandle / JS's Watch:
    interest add/remove/set_interest re-syncs the multiplex union live;
    mute_request suppresses one echo of a write this client already
    applied. `async with` for structured lifetime.

    FAN-OUT, not a shared queue: every call to `events()` (and every
    `async for` over the handle itself) gets its OWN buffer and sees the
    FULL event stream independently — mirrors sdk/js watch.js's `Watch`
    (a fresh CoalesceBuffer per `events`/`Symbol.asyncIterator` access,
    registered in a set, every push fans to all of them). Two consumers
    on one handle do NOT compete for the same event."""

    def __init__(self, client, interest, *, backpressure="coalesce",
                 buffer_size=100):
        self._client = client
        self._interest = normalize_interest(interest)
        self._opts = {"mode": backpressure, "size": buffer_size}
        self._sinks: set = set()      # every open consumer's buffer
        self._muted: dict = {}        # request-id -> monotonic expiry
        self._registered = False
        self._closed = False
        # a default sink so `async for ev in handle` works without an
        # explicit events() call, and internal composition (QueryWatch,
        # SqlTemplateWatch) has one dedicated buffer to read/manipulate.
        self._default_sink = self._new_sink()

    def _new_sink(self):
        buf = _AsyncCoalesceBuffer(mode=self._opts["mode"],
                                   size=self._opts["size"])
        if self._closed:
            buf.close()
        else:
            self._sinks.add(buf)
        return buf

    @property
    def interest(self):
        return self._interest

    async def __aenter__(self):
        await self.ready()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def register(self):
        """Join the multiplex union without waiting for the SSE to open —
        events start flowing once it does. ready() = register + wait."""
        self._registered = True
        await self._client._mux.register(self)

    async def ready(self, timeout=None):
        await self.register()
        await self._client._mux.wait_open(
            timeout=timeout or self._client._timeout)

    def add(self, xids):
        """Widen the record interest (live — re-syncs the union)."""
        if not xids:
            return
        current = list(self._interest.get("records") or [])
        seen = set(current)
        for x in xids:
            if x not in seen:
                seen.add(x)
                current.append(x)
        self._interest = dict(self._interest, records=current)
        self._client._mux.notify_interest_changed()

    def remove(self, xids):
        """Narrow the record interest (live)."""
        if not self._interest.get("records"):
            return
        drop = set(xids)
        self._interest = dict(
            self._interest,
            records=[x for x in self._interest["records"] if x not in drop])
        self._client._mux.notify_interest_changed()

    def set_interest(self, interest):
        """Replace the interest wholesale (live)."""
        self._interest = normalize_interest(interest)
        self._client._mux.notify_interest_changed()

    def mute_request(self, request_id, ttl=30.0):
        """Drop the NEXT event carrying this request-id (suppress the echo
        of a write this client already applied optimistically)."""
        if request_id:
            self._muted[request_id] = time.monotonic() + ttl

    def _push(self, ev):
        rid = ev.get("request")
        if rid and rid in self._muted:
            expiry = self._muted.pop(rid)
            if time.monotonic() < expiry:
                return                                # swallow one echo
        for buf in list(self._sinks):
            buf.push(ev)

    def _end(self):
        self._closed = True
        for buf in list(self._sinks):
            buf.close()

    def events(self):
        """Fresh async iterator with its own buffer — call as many times
        as you like; every call sees the FULL stream independently."""
        buf = self._new_sink()

        async def gen():
            try:
                while True:
                    ev = await buf.pop()
                    if ev is None:
                        return
                    yield ev
            finally:
                self._sinks.discard(buf)
        return gen()

    def __aiter__(self):
        return self

    async def __anext__(self):
        ev = await self._default_sink.pop()
        if ev is None:
            raise StopAsyncIteration
        return ev

    async def close(self):
        # no awaits — must survive an already-cancelled scope (see
        # mux.unregister)
        if self._registered:
            self._registered = False
            self._client._mux.unregister(self)
        self._end()


class AsyncWatchMultiplexer:
    """ONE shared SSE connection per client, fanned out to N watch handles
    via the pure matches()/shape_event() helpers, plus ONE consolidated
    POST /data/subscription/set (full set-replace, signature-deduped).
    This is the payoff: N open watches cost N buffers, not N threads.

    Contract (port of sdk/js watch.js / sdk/go watch.go):
      - The FIRST subscription flush is deferred until the SSE opens —
        POSTing earlier races the server's stream creation.
      - Every reconnect re-flushes (last-union cleared so the dedup cannot
        skip the re-POST) and fans out {"type": "connection/resumed"}.
      - runtime-model events refresh the schema resolver and fan out
        {"type": "schema/changed"} to watches AND schema sinks.
      - Empty union still POSTs [] to drop server-side state, UNLESS
        keep_alive (the pinned session keeps its state on purpose); an
        empty-watch flush still includes {"type": "runtime-model"} when
        SchemaWatch sinks are live."""

    def __init__(self, client, *, schema=True, keep_alive=False):
        self._client = client
        self._want_schema = schema
        self._keep_alive = keep_alive
        self._watches: set = set()
        self._schema_watches: set = set()
        self._task = None
        self._warm_task = None
        self._opened = asyncio.Event()
        self._flush_lock = asyncio.Lock()
        self._flush_pending = None
        self._flush_dirty = False
        self._last_union = None
        self.resolver = AsyncSchemaResolver(client) if schema else None

    def _ensure_task(self):
        if self._task is None:
            try:
                self._task = asyncio.ensure_future(self._run())
            except RuntimeError:
                return   # no running loop yet — next registration retries
            if self.resolver and not self.resolver.is_loaded():
                # Warm eagerly so events resolve from the first one;
                # failure is non-fatal (next ensure_loaded retries).
                self._warm_task = asyncio.ensure_future(self._warm_resolver())

    async def _warm_resolver(self):
        try:
            await self.resolver.ensure_loaded()
        except Exception:
            pass

    def _schedule_flush(self):
        """Detached, COALESCED flush: N rapid interest changes (e.g. a
        burst of stream closes) fold into one pending task, not N. The
        dirty flag makes a running flush go round again — it may already
        have snapshotted the watch set, and the tail change must not be
        the one that never reaches the server."""
        self._flush_dirty = True
        if self._flush_pending is None or self._flush_pending.done():
            self._flush_pending = asyncio.ensure_future(self._flush_safe())

    def _fatal(self, exc):
        """Auth failures are terminal: notify every watch, then END its
        stream (buffer close → StopAsyncIteration). Without this the task
        died silently and every watch went deaf forever."""
        sentinel = {"type": "subscription/rejected", "reason": str(exc),
                    "records": []}
        for h in list(self._watches):
            h._push(sentinel)
            h._end()
        for sw in list(self._schema_watches):
            sw._end()

    def _fan_out_sentinel(self, sentinel):
        for h in list(self._watches):
            h._push(sentinel)

    async def _run(self):
        delay = _INITIAL_BACKOFF
        last_event_id = None
        auth_retried = False
        first = True
        while True:
            try:
                token = await self._client._token()
                try:
                    reader, writer = await _open_sse(
                        self._client._endpoint, token, last_event_id,
                        self._client._timeout)
                except SynthigyError as e:
                    # Same clear+retry-once contract as the request leg: a
                    # cached token may have been rotated server-side. Only
                    # a SECOND consecutive 401 is fatal.
                    if (e.code == "UNAUTHORIZED"
                            and self._client._token_manager
                            and not auth_retried):
                        auth_retried = True
                        self._client._token_manager.clear()
                        continue
                    raise
                auth_retried = False
                try:
                    self._opened.set()
                    # First flush is deferred until here (the SSE open) —
                    # POSTing earlier races the server's stream creation.
                    # Every (re)connect re-flushes unconditionally: a
                    # reconnect means the server dropped its subscription
                    # state, so the dedup signature must be cleared or the
                    # re-POST gets skipped as a no-op change.
                    self._last_union = None
                    if not first:
                        self._fan_out_sentinel({"type": "connection/resumed"})
                    first = False
                    await self._flush_safe()
                    async for frame in _sse_frames(reader):
                        if frame.get("_sse_id"):
                            last_event_id = frame["_sse_id"]
                        delay = _INITIAL_BACKOFF
                        env = {k: v for k, v in frame.items()
                               if k not in ("_sse_event", "_sse_id")}
                        t = env.get("type")
                        if not t:
                            continue
                        if t == "runtime-model":
                            if self.resolver:
                                try:
                                    await self.resolver.refresh()
                                except Exception:
                                    continue
                                self._fan_out_sentinel({"type": "schema/changed"})
                                for sw in list(self._schema_watches):
                                    sw._push({"type": "schema/changed"})
                            continue
                        shaped = None
                        for h in list(self._watches):
                            if matches(h.interest, env):
                                if shaped is None:
                                    shaped = shape_event(env)
                                if shaped is not None:
                                    h._push(shaped)
                finally:
                    writer.close()
                    self._opened.clear()
            except SynthigyError as e:
                if e.code in ("UNAUTHORIZED", "FORBIDDEN"):
                    self._fatal(e)
                    raise
            except (OSError, ConnectionError):
                pass
            await asyncio.sleep(delay)
            delay = min(delay * 2, _MAX_BACKOFF)

    async def _flush_safe(self):
        while True:
            self._flush_dirty = False
            try:
                await self._flush()
            except SynthigyError as e:
                self._fan_out_sentinel({"type": "subscription/rejected",
                                        "reason": str(e), "records": []})
            if not self._flush_dirty:
                return

    async def _flush(self):
        """POST the union of every open watch's interest (full set-replace
        via client.set_subscriptions, which also keeps the client's local
        subscription mirror coherent). Signature-deduped so an unchanged
        union never re-POSTs."""
        async with self._flush_lock:
            watches = list(self._watches)
            schema_sinks = bool(self._schema_watches)
            if not watches:
                if self._keep_alive:
                    return  # pinned session keeps its server-side state
                items = ([{"type": "runtime-model"}]
                         if (self._want_schema and schema_sinks) else [])
            else:
                records, entities, relations, ops = set(), set(), set(), set()
                any_ops = False
                for h in watches:
                    interest = h.interest
                    records.update(interest.get("records") or [])
                    entities.update(interest.get("entities") or [])
                    relations.update(interest.get("relations") or [])
                    if interest.get("ops"):
                        any_ops = True
                        ops.update(interest["ops"])
                items = []
                if records:
                    item = {"type": "data", "records": sorted(records)}
                    if any_ops:
                        item["operations"] = sorted(ops)
                    items.append(item)
                if entities:
                    items.append({"type": "entity", "entities": sorted(entities)})
                if relations:
                    items.append({"type": "relation",
                                  "relations": sorted(relations)})
                if self._want_schema:
                    items.append({"type": "runtime-model"})
            sig = json.dumps(items)
            if sig == self._last_union:
                return
            await self._client.set_subscriptions(items)
            # Record the signature only AFTER the POST succeeds. Recording
            # it first (thread-mux ordering) let a cancellation between the
            # two leave the server's set stale while the dedup skipped
            # every retry of the same union — threads can't be cancelled
            # mid-function, coroutines can.
            self._last_union = sig

    async def register(self, handle):
        self._ensure_task()
        self._watches.add(handle)
        # If the SSE hasn't opened yet, _run's post-open flush picks this
        # registration up for free — flushing here too would just be a
        # deduped no-op POST attempt racing stream creation.
        if self._opened.is_set():
            await self._flush_safe()

    def unregister(self, handle):
        """Synchronous ON PURPOSE — no awaits. Watch close() runs in a
        stream's `finally`, which on browser abort executes inside an
        already-cancelled scope where every await raises CancelledError
        immediately. A no-await body always completes: the discard (what
        prevents the leak) is unconditional; the server-side narrowing
        flush is detached so cancellation can neither skip nor kill it."""
        if handle not in self._watches:
            return
        self._watches.discard(handle)
        self._schedule_flush()

    async def register_schema(self, sw):
        self._ensure_task()
        self._schema_watches.add(sw)
        if self._opened.is_set():
            await self._flush_safe()

    def unregister_schema(self, sw):
        self._schema_watches.discard(sw)

    def notify_interest_changed(self):
        """A handle's interest mutated (add/remove/set_interest) — re-sync
        the union. Detached for the same cancellation-safety reason as
        unregister; deferred to the post-open flush when SSE isn't up yet."""
        if self._opened.is_set():
            self._schedule_flush()

    async def wait_open(self, timeout=None):
        await asyncio.wait_for(self._opened.wait(), timeout=timeout)

    async def close(self):
        # End every stream so consumers unblock (queued events drain).
        for h in list(self._watches):
            h._end()
        for sw in list(self._schema_watches):
            sw._end()
        for t in (self._warm_task, self._flush_pending):
            if t is not None and not t.done():
                t.cancel()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None


def _prov(trigger):
    """Provenance (ts/txid/actor/request) carried from the trigger event
    onto derived events."""
    out = {}
    if trigger:
        for k in ("ts", "txid", "actor", "request"):
            if trigger.get(k) is not None:
                out[k] = trigger[k]
    return out


class AsyncQueryWatch:
    """Live result set with DERIVED events: iteration yields query/added,
    query/changed (before/after/changed diff + provenance) and
    query/removed, computed by refetching after a coalesced trigger
    window (notify-then-refetch through the IAM-filtered read path —
    RLS-correct). Interest re-syncs to the live result set after every
    refresh, so rows that enter the window get watched and dropped ones
    stop firing; the entity's relation xids (schema resolver) ride along
    as a local matcher aid; an empty snapshot falls back to entity-touch
    interest so new rows still wake the query."""

    def __init__(self, client, kind, query, *, acting_as=None):
        """kind: "search" -> query={entity,args,selection}
                 "xsql"   -> query={xsql,params,entity} (entity is the
                             wire entity name; kebab-cased for the
                             entities-fallback interest)"""
        self._client = client
        self._kind = kind
        self._query = query
        self._acting_as = acting_as
        self._handle = None
        self._records: dict = {}
        self._initial_rows = None
        self._pending: list = []
        self._relation_xids: list = []

    async def __aenter__(self):
        await self.ready()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def _run_query(self):
        q = self._query
        if self._kind == "xsql":
            r = await self._client._query_xsql(q["xsql"], q.get("params"),
                                               acting_as=self._acting_as)
            return r if isinstance(r, list) else ([r] if r else [])
        return await self._client._search(q["entity"], q.get("args"),
                                          q.get("selection"),
                                          acting_as=self._acting_as)

    def _interest_for(self, rows):
        interest = {"records": sorted(_collect_all_xids(rows))}
        if self._relation_xids:
            interest["relation_xids"] = list(self._relation_xids)
        if not interest["records"] and not self._relation_xids:
            # Empty snapshot: fall back to the entity-touch track so new
            # rows still wake the query.
            interest = {"entities": [kebab(self._query["entity"])]}
        return interest

    async def ready(self, timeout=None):
        resolver = self._client._mux.resolver
        if resolver:
            await resolver.ensure_loaded()
            entity_xid = resolver.entity_xid_by_name(self._query.get("entity"))
            self._relation_xids = (resolver.relations_for_entity(entity_xid)
                                   if entity_xid else [])
        rows = await self._run_query()
        self._records = {k: r for r in rows
                         if (k := _row_key(r)) is not None}
        self._initial_rows = rows
        self._handle = AsyncWatchHandle(self._client,
                                        self._interest_for(rows))
        await self._handle.ready(timeout=timeout)

    def initial(self):
        """First page of rows, server order. None until ready."""
        return self._initial_rows

    @property
    def records(self):
        """Live xid -> record map, updated by refreshes."""
        return self._records

    def list(self):
        return list(self._records.values())

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            if self._pending:
                return self._pending.pop(0)
            trigger = await self._handle.__anext__()  # StopAsyncIteration ends us
            t = trigger.get("type")
            if t in ("subscription/rejected", "paused", "connection/resumed",
                     "schema/changed"):
                return trigger                        # sentinels pass through
            # Coalesce the burst: a write fanning out several events should
            # cost ONE refetch.
            await asyncio.sleep(REFRESH_COALESCE)
            sink = self._handle._default_sink
            drained = sink._queue
            sink._queue = [e for e in drained
                          if (e.get("type") or "")
                          .startswith(("subscription/", "paused",
                                       "connection/", "schema/"))]
            sink._reindex()
            self._pending.extend(await self._do_refresh(trigger))

    async def refresh(self):
        """Force a fresh server-side re-evaluation; derived events surface
        on the iterator."""
        self._pending.extend(await self._do_refresh(None))

    async def _do_refresh(self, trigger):
        rows = await self._run_query()
        prev = self._records
        nxt = {k: r for r in rows if (k := _row_key(r)) is not None}
        self._records = nxt
        prov = _prov(trigger)
        events = []
        for xid, r in nxt.items():
            before = prev.get(xid)
            if before is None:
                events.append(dict({"type": "query/added", "record": r},
                                   **prov))
            elif not _rows_equal(before, r):
                changed, bmap, amap = _diff_row(before, r)
                events.append(dict({"type": "query/changed", "record": r,
                                    "before": bmap, "after": amap,
                                    "changed": changed}, **prov))
        for xid in prev:
            if xid not in nxt:
                events.append(dict({"type": "query/removed", "record": xid},
                                   **prov))
        # Re-sync the interest so new members are watched and dropped ones
        # stop firing (falls back to entity-touch while the set is empty).
        new_interest = self._interest_for(rows)
        if new_interest != self._handle.interest:
            self._handle.set_interest(new_interest)
        return events

    async def close(self):
        if self._handle is not None:
            # close() is await-free under the hood — cancellation-safe
            await self._handle.close()
            self._handle = None


class AsyncEntityWatch:
    """Raw entity-touch poke channel: yields every entity/touched event for
    the named entities. No query, no snapshot — the consumer decides what a
    poke means (typically: refetch with its own current parameters).
    FAN-OUT: `events()` (and `async for` directly) each get their own
    stream — see AsyncWatchHandle.events()."""

    def __init__(self, client, entities):
        self._client = client
        self._entities = list(entities)
        self._handle = None

    async def __aenter__(self):
        await self.ready()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def ready(self, timeout=None):
        self._handle = AsyncWatchHandle(self._client,
                                        {"entities": self._entities})
        await self._handle.ready(timeout=timeout)

    def events(self):
        """Fresh async iterator — call as many times as you like; every
        call sees the FULL stream independently."""
        return self._handle.events()

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self._handle.__anext__()

    async def close(self):
        if self._handle is not None:
            await self._handle.close()
            self._handle = None


class AsyncSqlTemplateWatch:
    """Live raw-SQL result. Raw SQL is opaque, so the caller declares which
    entities/relations/records the SQL reads from; only surfaces
    result/changed when the re-run actually differs."""

    def __init__(self, client, template, params=None, *, entities=None,
                 relations=None, records=None, acting_as=None):
        self._client = client
        self._template = template
        self._params = params
        self._acting_as = acting_as
        self._entities = list(entities) if entities else []
        self._relations = list(relations) if relations else []
        self._records_opt = list(records) if records else []
        if not self._entities and not self._relations:
            raise SynthigyError(
                "watch_sql_template requires entities= and/or relations= — "
                "list the entity names and/or relation names the SQL reads "
                "from", "INVALID_INTEREST")
        self._value = None
        self._handle = None

    async def __aenter__(self):
        await self.ready()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def _fetch(self):
        return await self._client._sql_template(self._template, self._params,
                                                acting_as=self._acting_as)

    async def ready(self, timeout=None):
        self._value = await self._fetch()
        interest = {}
        if self._entities:
            interest["entities"] = list(self._entities)
        if self._relations:
            interest["relations"] = list(self._relations)
        if self._records_opt:
            interest["records"] = list(self._records_opt)
        self._handle = AsyncWatchHandle(self._client, interest)
        await self._handle.ready(timeout=timeout)

    def value(self):
        return self._value

    def first(self):
        v = self._value
        return v[0] if isinstance(v, list) and v else None

    def __aiter__(self):
        return self

    async def __anext__(self):
        # An underlying event just pokes; only surface result/changed if
        # the re-run actually differs — a touch that doesn't move this
        # template's result is silently swallowed, not forwarded.
        while True:
            ev = await self._handle.__anext__()
            t = ev.get("type")
            if t in ("subscription/rejected", "paused", "connection/resumed",
                     "schema/changed"):
                return ev
            before = self._value
            after = await self._fetch()
            self._value = after
            if after != before:
                return dict({"type": "result/changed", "before": before,
                             "after": after}, **_prov(ev))

    async def close(self):
        if self._handle is not None:
            await self._handle.close()
            self._handle = None


class AsyncSchemaWatch:
    """Yields {"type": "schema/changed"} each time a deploy lands. Most
    apps don't need this — watch() events already track the live schema."""

    def __init__(self, client):
        self._client = client
        self._buf = _AsyncCoalesceBuffer(mode="lossless", size=32)
        self._registered = False

    async def __aenter__(self):
        await self.ready()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def ready(self, timeout=None):
        self._registered = True
        await self._client._mux.register_schema(self)
        await self._client._mux.wait_open(
            timeout=timeout or self._client._timeout)

    def _push(self, ev):
        if ev.get("type") == "schema/changed":
            self._buf.push(ev)

    def _end(self):
        self._buf.close()

    def __aiter__(self):
        return self

    async def __anext__(self):
        ev = await self._buf.pop()
        if ev is None:
            raise StopAsyncIteration
        return ev

    async def close(self):
        # no awaits — cancellation-safe (see mux.unregister)
        if self._registered:
            self._registered = False
            self._client._mux.unregister_schema(self)
        self._buf.close()


class AsyncHistoryAPI:
    """Temporal query API — POST /history over the audit plug. Wire
    opts are KEBAB-keyed (record-xid, from-ts, group-by, include-deleted?).
    404 = no audit provider configured -> HISTORY_UNAVAILABLE."""

    def __init__(self, client):
        self._client = client

    async def _post(self, op, opts):
        status, _h, data = await self._client._authed(
            "POST", "/history",
            body=json.dumps({"op": op, "opts": opts}).encode(),
            headers={"Content-Type": "application/json"})
        if status == 404:
            raise SynthigyError(
                "History endpoint unavailable — no audit provider configured "
                "on the server", "HISTORY_UNAVAILABLE")
        if not 200 <= status < 300:
            try:
                parsed = json.loads(data)
            except ValueError:
                parsed = {}
            err = parsed.get("error") or {} if isinstance(parsed, dict) else {}
            raise SynthigyError(
                err.get("message", f"History op '{op}' failed ({status})"),
                err.get("code", "HTTP_ERROR"))
        return json.loads(data).get("result")

    async def get_at(self, record_xid, at, *, tenant=None, include_deleted=None):
        opts = {"record-xid": record_xid, "at": at}
        if tenant is not None:
            opts["tenant"] = tenant
        if include_deleted is not None:
            opts["include-deleted?"] = include_deleted
        return await self._post("get-at", opts)

    async def events(self, *, record_xid=None, between=None, tenant=None,
                     limit=None, track=None):
        # server requires an upper bound; default = "recent events up to now"
        opts = {"between": between if between is not None
                else [None, now_iso()]}
        if record_xid is not None:
            opts["record-xid"] = record_xid
        if tenant is not None:
            opts["tenant"] = tenant
        if limit is not None:
            opts["limit"] = limit
        if track is not None:
            opts["track"] = track
        return await self._post("events", opts)

    async def diff(self, record_xid, from_ts, to_ts, *, tenant=None):
        opts = {"record-xid": record_xid, "from-ts": from_ts, "to-ts": to_ts}
        if tenant is not None:
            opts["tenant"] = tenant
        return await self._post("diff", opts)

    async def timeline(self, *, between=None, group_by=None, tenant=None,
                       limit=None):
        opts = {"between": between if between is not None
                else [None, now_iso()]}
        if group_by is not None:
            opts["group-by"] = group_by
        if tenant is not None:
            opts["tenant"] = tenant
        if limit is not None:
            opts["limit"] = limit
        return await self._post("timeline", opts)

    async def since(self, *, cursor=None, tenant=None, limit=None, track=None):
        opts = {}
        if cursor is not None:
            opts["cursor"] = cursor
        if tenant is not None:
            opts["tenant"] = tenant
        if limit is not None:
            opts["limit"] = limit
        if track is not None:
            opts["track"] = track
        return await self._post("since", opts)


def _parse_onboard_response(data, status, label):
    """Shared response handling for /oauth/onboard and
    /oauth/onboard/complete. Wire shape on error here is
    {"error": "<snake_case_code>"} — a bare string, unlike the
    {"error": {"code", "message"}} envelope /data uses — so it's handled
    separately rather than through error_from_server (which would silently
    miss the code on a string)."""
    try:
        parsed = json.loads(data) if data else None
    except ValueError:
        parsed = None
    if not 200 <= status < 300:
        err = (parsed or {}).get("error") if isinstance(parsed, dict) else None
        if err:
            raise SynthigyError(f"{label} failed: {err}", err.upper(),
                                status=status)
        raise SynthigyError(f"{label} request failed ({status})",
                            "HTTP_ERROR", data.decode(errors="replace"),
                            status=status)
    return parsed


class AsyncClient:
    """Async-native Synthigy client — the engine. Reads, XSQL,
    sql-template, writes, batch, trees, history, schema/lint, raw
    listen/observe streaming, subscriptions, and the full watch family,
    all with acting_as identity multiplexing (client default, per-call
    override).

    on_op: optional callback invoked after every HTTP call with
    {"path", "status", "duration_ms"} — the observability seam; wire it to
    Prometheus/OTel yourself, the SDK takes no dependency."""

    def __init__(self, endpoint, *, client_id=None, client_secret=None,
                 token=None, scope=None, audience=None, acting_as=None,
                 key_format=None, timeout=None, keep_alive=False, on_op=None):
        """
        endpoint     : Synthigy base URL (required)
        client_id/client_secret : OAuth client-credentials mode
        token        : static bearer token (empty string allowed — dev/authless)
        scope        : OAuth scope for client-credentials mode
        audience     : default audience bound to every client-credentials
                       mint (defaults to $SYNTHIGY_AUDIENCE). The platform's
                       audience model is opt-in: a mint naming none resolves
                       to the identity-only OIDC audience that /data rejects.
                       The server publishes its /data audience at
                       /.well-known/synthigy as auth.oidc.audience.
        acting_as    : default impersonation, overridable per call
        key_format   : "kebab" | "snake" | "camel"; None = server default (snake)
        timeout      : per-request timeout in SECONDS (client default;
                       verbs also take a per-call timeout=)
        keep_alive   : pin the SSE session open across watch churn
                       (needs a running event loop at construction)
        """
        self._endpoint = endpoint.rstrip("/")
        self._data_url = self._endpoint + "/data"
        self._default_acting_as = acting_as
        self._default_key_format = key_format
        self._timeout = timeout
        self._keep_alive = keep_alive
        self._on_op = on_op
        self._pool = _AsyncPool(timeout=timeout)
        self._default_audience = audience or os.environ.get("SYNTHIGY_AUDIENCE")

        # Token source resolution — PLAN-EXEC-IDENTITY.md step 3, in order:
        # caller-supplied (code) -> client credentials (code) ->
        # SYNTHIGY_SUPERVISED stdio ask -> SYNTHIGY_TOKEN env -> the teaching
        # throw. The pipe beats the env var deliberately: exec injects the
        # cached token AND supervises, and the env snapshot can't refresh
        # mid-run — the pipe can. The throw IS the UX: no flag, no silent
        # anonymous fallback.
        if token is not None:
            # Static token mode — empty string allowed for unauth/dev servers.
            self._token_manager = None
            self._static_token = token
        elif client_id and client_secret:
            from .auth import TokenManager
            self._token_manager = TokenManager(
                self._endpoint + "/oauth/token", client_id, client_secret,
                scope=scope, timeout=timeout)
            self._static_token = None
        elif os.environ.get("SYNTHIGY_SUPERVISED") == "1":
            from .auth import SupervisedTokenSource
            self._token_manager = SupervisedTokenSource()
            self._static_token = None
        elif os.environ.get("SYNTHIGY_TOKEN"):
            # A snapshot, not a live source: exec/connect refresh and
            # rewrite the profile's cache on THEIR next run, not this
            # process's — a script outliving its token's TTL is a known
            # limit of this mode (supervised mode is the one built for
            # long-running processes needing repeated fresh tokens).
            self._token_manager = None
            self._static_token = os.environ["SYNTHIGY_TOKEN"]
        else:
            from .auth import no_token_error
            raise no_token_error()

        # Local mirror of the server subscription set (full set-replace
        # wire) — shared by the raw subscriptions API, observe() and the
        # watch multiplexer's union flush.
        self._data_subs = {}       # key -> {"records": set, "operations": set?}
        self._model_subs = set()   # "deployed-model" | "runtime-model"
        self._entity_subs = set()
        self._relation_subs = set()

        self._mux = AsyncWatchMultiplexer(self, keep_alive=keep_alive)
        if keep_alive:
            # Pin the SSE session eagerly so the plug session keyed by
            # [sub, client_id] lands before the first user watch races to
            # use it. No-op when no loop is running yet.
            self._mux._ensure_task()

    async def _token(self):
        if self._token_manager is None:
            return self._static_token
        # Refresh is infrequent + cached (TokenManager holds a 30s-buffer
        # cache) — bridging via to_thread is fine; it's not on the hot
        # idle-watch path.
        return await asyncio.to_thread(self._token_manager.get_token,
                                       self._default_audience)

    async def token(self, audience=None):
        """Access token for `audience`, falling back to the client's
        configured audience (Synthigy-as-IdP). Static mode returns the
        static token regardless of audience."""
        if self._token_manager is None:
            return self._static_token
        return await asyncio.to_thread(self._token_manager.get_token,
                                       audience or self._default_audience)

    async def _authed(self, method, path, body=None, headers=None):
        """Authorized request to any endpoint: bearer + request id, 401
        clear+retry-once, on_op timing. -> (status, headers, body-bytes)."""
        url = self._endpoint + path
        h = dict(headers or {})
        h.setdefault("X-Request-Id", generate_request_id())
        token = await self._token()
        if token:
            h["Authorization"] = f"Bearer {token}"
        started = time.monotonic()
        status, resp_headers, data = await self._pool.request(
            method, url, body=body, headers=h)
        if status == 401 and self._token_manager:
            self._token_manager.clear()
            h["Authorization"] = f"Bearer {await self._token()}"
            status, resp_headers, data = await self._pool.request(
                method, url, body=body, headers=h)
        if self._on_op:
            try:
                self._on_op({"path": path, "status": status,
                             "duration_ms": (time.monotonic() - started) * 1000})
            except Exception:
                pass                                   # observer must not break ops
        if status == 401:
            raise SynthigyError("Unauthorized", "UNAUTHORIZED",
                                request_id=resp_headers.get("x-request-id"),
                                status=401)
        return status, resp_headers, data

    async def _post(self, body):
        """POST to /data; maps 403 / non-2xx to SynthigyError; returns
        parsed JSON with _request_id attached."""
        status, resp_headers, data = await self._authed(
            "POST", "/data", body=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        request_id = resp_headers.get("x-request-id")
        try:
            parsed = json.loads(data) if data else None
        except ValueError:
            parsed = None
        if status == 403:
            err = parsed.get("error") if isinstance(parsed, dict) else None
            raise error_from_server(
                err or {"message": "Forbidden", "code": "FORBIDDEN"},
                403, request_id)
        if not 200 <= status < 300:
            err = parsed.get("error") if isinstance(parsed, dict) else None
            if err:
                raise error_from_server(err, status, request_id)
            raise SynthigyError(f"Request failed ({status})", "HTTP_ERROR",
                                data.decode(errors="replace"),
                                request_id=request_id, status=status)
        if isinstance(parsed, dict) and request_id:
            parsed["_request_id"] = request_id
        return parsed

    # ── exec + verbs ───────────────────────────────────────────────────

    async def exec_(self, operations, *, acting_as=None, key_format=None,
                    timeout=None):
        """Execute raw operations in one round trip; returns per-op results
        (the @batch/overview wire shape). acting_as multiplexes identity:
        per-call value overrides the client default; a BFF passes the
        logged-in user's xid so every op runs under THEIR permissions, not
        the service client's. timeout= is a per-call deadline overriding
        the client-level one."""
        body = {"operations": operations}
        aa = acting_as if acting_as is not None else self._default_acting_as
        if aa:
            body["acting_as"] = aa
        kf = key_format if key_format is not None else self._default_key_format
        if kf:
            body["key_format"] = kf
        coro = self._post(body)
        try:
            resp = await (asyncio.wait_for(coro, timeout) if timeout else coro)
        except asyncio.TimeoutError:
            raise SynthigyError("request timed out", "TIMEOUT") from None
        request_id = resp.get("_request_id") if isinstance(resp, dict) else None
        if resp.get("error"):
            raise error_from_server(resp["error"], None, request_id)
        results = resp.get("results") or []
        if request_id:
            for r in results:
                if isinstance(r, dict):
                    r["_request_id"] = request_id
        return results

    async def _one(self, op, **opts):
        result = ((await self.exec_([op], **opts)) or [{}])[0]
        if not result.get("ok"):
            raise error_from_server(result.get("error"), None,
                                    result.get("_request_id"))
        return result.get("data")

    async def _search(self, entity, args, selection, **opts):
        data = await self._one({"op": "search", "entity": entity, "args": args,
                                "selections": normalize_selection(selection)},
                               **opts)
        return data if data is not None else []

    async def search(self, entity, args=None, selection=None, **opts):
        return await self._search(entity, args, selection, **opts)

    async def get(self, entity, args=None, selection=None, **opts):
        # get takes FLAT unique-constraint values — no _eq wrapping, no _where
        return await self._one({"op": "get", "entity": entity, "args": args,
                                "selections": normalize_selection(selection)},
                               **opts)

    async def _query_xsql(self, xsql, params=None, *, op="search", **opts):
        """XSQL query — sends the `xsql` DOCUMENT op (the only XSQL wire
        shape): {op: "xsql", xsql: <document>, params}. A bare rooted body
        gets a synthetic `@<op> _q` header client-side; `op` also picks the
        unwrap ("get" returns a single record or None). The server derives
        verb/entity/selections/args from the document."""
        o = {"op": "xsql", "xsql": xsql_document(xsql, op)}
        if params is not None:
            o["params"] = params
        data = await self._one(o, **opts)
        if op == "get":
            return data
        return data if data is not None else []

    async def query(self, xsql, params=None, *, op="search", **opts):
        return await self._query_xsql(xsql, params, op=op, **opts)

    async def _sql_template(self, template, params=None, **opts):
        data = await self._one({"op": "sql-template", "template": template,
                                "params": params or []}, **opts)
        return data if data is not None else []

    async def sql_template(self, template, params=None, **opts):
        return await self._sql_template(template, params, **opts)

    # ── writes ─────────────────────────────────────────────────────────

    async def sync(self, entity, data, *, returning=False, **opts):
        """Upsert. Silent by default — answers {"count": n}; pass
        returning=True for the written records, or mint ids up front with
        new_xid(), which is the cheap way to know what you wrote."""
        return await self._one({"op": "sync", "entity": entity,
                                "data": data,
                                "returning": bool(returning)}, **opts)

    async def stack(self, entity, data, *, returning=False, **opts):
        """Additive upsert. Same returning contract as sync."""
        return await self._one({"op": "stack", "entity": entity,
                                "data": data,
                                "returning": bool(returning)}, **opts)

    async def slice(self, entity, args, selection=None, **opts):
        return await self._one(
            {"op": "slice", "entity": entity, "args": args,
             "selections": normalize_selection(selection)},
            **opts)

    async def delete(self, entity, data, **opts):
        return await self._one({"op": "delete", "entity": entity,
                                "data": data}, **opts)

    async def purge(self, entity, args, selection=None, **opts):
        return await self._one(
            {"op": "purge", "entity": entity, "args": args,
             "selections": normalize_selection(selection)},
            **opts)

    # ── trees / models / introspection / history ───────────────────────

    async def search_tree(self, entity, on, args=None, selection=None, *,
                          raw=False, children_key="_children", **opts):
        flat = await self._one(
            {"op": "search-tree", "entity": entity, "on": on, "args": args,
             "selections": normalize_selection(selection)},
            **opts)
        flat = flat if flat is not None else []
        if raw:
            return flat
        return compose_forest(flat, on, children_key=children_key)

    async def get_tree(self, entity, root, on, selection=None, *, raw=False,
                       children_key="_children", **opts):
        flat = await self._one(
            {"op": "get-tree", "entity": entity, "root": root, "on": on,
             "selections": normalize_selection(selection)},
            **opts)
        flat = flat if flat is not None else []
        if raw:
            return flat
        return compose_tree(flat, on, root_id=root, children_key=children_key)

    async def deployed_model(self, **opts):
        return await self._one({"op": "deployed-model"}, **opts)

    async def runtime_model(self, **opts):
        return await self._one({"op": "runtime-model"}, **opts)

    async def schema(self, entities=None):
        """IAM-filtered schema (GET /schema). `entities` narrows the pull."""
        path = "/schema"
        if entities:
            path += "?" + urllib.parse.urlencode({"entities": ",".join(entities)})
        status, _h, data = await self._authed("GET", path)
        if not 200 <= status < 300:
            raise SynthigyError(f"schema request failed ({status})",
                                "HTTP_ERROR", status=status)
        return json.loads(data)

    async def lint(self, source, *, entity=None, op=None):
        """Lint an XSQL source string (POST /lint) -> list of diagnostics."""
        body = {"source": source}
        if entity:
            body["entity"] = entity
        if op:
            body["op"] = op
        status, _h, data = await self._authed(
            "POST", "/lint", body=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        if not 200 <= status < 300:
            raise SynthigyError(f"lint request failed ({status})",
                                "HTTP_ERROR", status=status)
        return json.loads(data).get("diagnostics", [])

    async def onboard(self, xid, *, reset=None, methods=None,
                      ttl_seconds=None, return_url=None):
        """Mint a one-time account-claim link (POST /oauth/onboard).
        Confidential client whose principal administers the account — RBAC
        update on User plus the row inside its owner-group write scope, which
        the shipped User Provisioner role grants — else PROVISION_FORBIDDEN.
        This client's own client_credentials identity IS that principal, so
        no separate credential is needed here.

        xid         : an EXISTING account's xid. Onboarding no longer creates
                      accounts — create it first via sync() (with
                      person_info, roles, groups in one tree), then mint a
                      ticket for it. A blank xid raises XID_REQUIRED; one
                      that doesn't resolve raises USER_NOT_FOUND.
        reset       : soft-recycle the account first — strip its federated
                      identities, null its password, revoke its live
                      sessions/tokens — before minting. Does NOT touch
                      `active`; that flag is your data, write it yourself.
        methods     : restrict the claim page, e.g. ["password"] or ["google"];
                      omitted = every active federation provider plus password
        ttl_seconds : claim-link lifetime; server default 24h
        return_url  : where a successful DIRECT (browser) claim redirects
                      instead of Synthigy's generic status page. Must match
                      one of THIS client's registered redirections (or be a
                      loopback URI).

        -> {"onboard_url": "...", "expires_at": <epoch ms>, "user": {"xid": "..."}}
        Raises SynthigyError (.code one of PROVISION_FORBIDDEN,
        XID_REQUIRED, USER_NOT_FOUND, RETURN_URL_NOT_REGISTERED) on
        failure."""
        body = {"xid": xid}
        if reset is not None:
            body["reset"] = reset
        if methods is not None:
            body["methods"] = list(methods)
        if ttl_seconds is not None:
            body["ttl_seconds"] = ttl_seconds
        if return_url is not None:
            body["return_url"] = return_url
        status, _h, data = await self._authed(
            "POST", "/oauth/onboard", body=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        return _parse_onboard_response(data, status, "onboard")

    async def onboard_complete(self, ticket):
        """Redeem an onboarding ticket without a browser
        (POST /oauth/onboard/complete) — the indirect face of the SAME
        ticket onboard() mints. Must be called by the SAME client that
        minted the ticket; any other client's bearer is rejected with
        CLAIM_INVALID, and the client must still administer the account
        (PROVISION_FORBIDDEN).

        The caller runs its own out-of-band proofing (email link, SMS OTP,
        push approval, KYC, a phone call — Synthigy never learns which) and,
        once satisfied, redeems the ticket itself instead of bouncing the
        user's browser through /oauth/claim. This never sets a credential —
        credentials are subject-only. The account activates with none; give
        it one via the claim page (password or a federated identity) or a
        later ticket.

        ticket : the token minted by onboard() (parse it out of onboard_url)

        -> {"user": {"xid": "..."}, "active": true}
        Raises SynthigyError (.code CLAIM_INVALID) on failure."""
        body = {"ticket": ticket}
        status, _h, data = await self._authed(
            "POST", "/oauth/onboard/complete", body=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        return _parse_onboard_response(data, status, "onboard-complete")

    @property
    def history(self):
        """Temporal query API (POST /history): get_at / events / diff /
        timeline / since."""
        if not hasattr(self, "_history"):
            self._history = AsyncHistoryAPI(self)
        return self._history

    # ── subscriptions (advanced — prefer the watch family) ─────────────
    # Raw calls here clobber a live watch multiplexer's union: the mux
    # flushes THROUGH set_subscriptions, and the server set is a full
    # replace every time.

    def _build_subscriptions_body(self):
        items = []
        for d in self._data_subs.values():
            item = {"type": "data", "records": sorted(d["records"])}
            if "operations" in d:
                item["operations"] = sorted(d["operations"])
            items.append(item)
        if self._entity_subs:
            items.append({"type": "entity", "entities": sorted(self._entity_subs)})
        if self._relation_subs:
            items.append({"type": "relation",
                          "relations": sorted(self._relation_subs)})
        for t in self._model_subs:
            items.append({"type": t})
        return {"subscriptions": items}

    async def _flush_subscriptions(self):
        body = self._build_subscriptions_body()
        status, _h, data = await self._authed(
            "POST", "/data/subscription/set",
            body=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        try:
            parsed = json.loads(data) if data else {}
        except ValueError:
            parsed = {}
        if not 200 <= status < 300:
            err = parsed.get("error") or {} if isinstance(parsed, dict) else {}
            raise SynthigyError(err.get("message", "Subscription set failed"),
                                err.get("code", "HTTP_ERROR"))
        return parsed

    async def _flush_subscriptions_safe(self):
        try:
            await self._flush_subscriptions()
        except SynthigyError:
            pass

    async def subscribe(self, descriptor, *, key=None):
        """Add a record descriptor to the set and POST the full set."""
        normalized = normalize_descriptor(descriptor)
        key = key or descriptor_key(normalized)
        self._data_subs[key] = normalized
        return await self._flush_subscriptions()

    async def unsubscribe(self, handle):
        """Remove by subscribe-time key or an equal-by-hash descriptor."""
        key = None
        if isinstance(handle, str):
            if handle in self._data_subs:
                key = handle
            else:
                fallback = descriptor_key({"records": {handle}})
                if fallback in self._data_subs:
                    key = fallback
        else:
            key = descriptor_key(normalize_descriptor(handle))
        if key is None or self._data_subs.pop(key, None) is None:
            return {"ok": True}
        return await self._flush_subscriptions()

    async def subscribe_model(self, *, raw=False):
        self._model_subs.add("deployed-model" if raw else "runtime-model")
        return await self._flush_subscriptions()

    async def unsubscribe_model(self, *, raw=False):
        t = "deployed-model" if raw else "runtime-model"
        if t not in self._model_subs:
            return {"ok": True}
        self._model_subs.discard(t)
        return await self._flush_subscriptions()

    async def set_subscriptions(self, items):
        """Full replace of the local mirror + server set in one call."""
        self._data_subs.clear()
        self._model_subs.clear()
        self._entity_subs.clear()
        self._relation_subs.clear()
        for item in items:
            t = item.get("type", "data")
            if t == "data":
                d = {"records": item.get("records")}
                if "operations" in item:
                    d["operations"] = item["operations"]
                normalized = normalize_descriptor(d)
                self._data_subs[item.get("key") or
                                descriptor_key(normalized)] = normalized
            elif t == "entity":
                if not isinstance(item.get("entities"), (list, tuple)):
                    raise SynthigyError(
                        "entity subscription requires entities array",
                        "INVALID_SUBSCRIPTION")
                self._entity_subs.update(item["entities"])
            elif t == "relation":
                if not isinstance(item.get("relations"), (list, tuple)):
                    raise SynthigyError(
                        "relation subscription requires relations array",
                        "INVALID_SUBSCRIPTION")
                self._relation_subs.update(item["relations"])
            elif t in ("deployed-model", "runtime-model"):
                self._model_subs.add(t)
            else:
                raise SynthigyError(f"Unsupported subscription type: {t}",
                                    "UNSUPPORTED_TYPE")
        return await self._flush_subscriptions()

    async def clear_subscriptions(self):
        self._data_subs.clear()
        self._model_subs.clear()
        self._entity_subs.clear()
        self._relation_subs.clear()
        return await self._flush_subscriptions()

    async def subscriptions(self):
        """Current server-side subscription status (GET, not the mirror)."""
        status, _h, data = await self._authed("GET", "/data/subscription/status")
        try:
            parsed = json.loads(data) if data else {}
        except ValueError:
            parsed = {}
        if not 200 <= status < 300:
            err = parsed.get("error") or {} if isinstance(parsed, dict) else {}
            raise SynthigyError(err.get("message", "Subscription status failed"),
                                err.get("code", "HTTP_ERROR"))
        return parsed

    # ── raw streaming ──────────────────────────────────────────────────

    async def listen(self):
        """Infinite reconnect loop over the raw SSE channel. Yields the
        channel envelope verbatim; the open sentinel surfaces as
        {"type": "sse/open"}. Backoff 1s doubling to 30s, reset on any
        frame; Last-Event-ID resume across reconnects. UNAUTHORIZED /
        FORBIDDEN are fatal and raise (single 401 gets one clear+retry)."""
        delay = _INITIAL_BACKOFF
        last_event_id = None
        auth_retried = False
        while True:
            try:
                token = await self._token()
                try:
                    reader, writer = await _open_sse(self._endpoint, token,
                                                     last_event_id,
                                                     self._timeout)
                except SynthigyError as e:
                    if (e.code == "UNAUTHORIZED" and self._token_manager
                            and not auth_retried):
                        auth_retried = True
                        self._token_manager.clear()
                        continue
                    raise
                auth_retried = False
                try:
                    yield {"type": "sse/open"}
                    async for frame in _sse_frames(reader):
                        if frame.get("_sse_id"):
                            last_event_id = frame["_sse_id"]
                        delay = _INITIAL_BACKOFF
                        yield {k: v for k, v in frame.items()
                               if k not in ("_sse_event", "_sse_id")}
                finally:
                    writer.close()
            except SynthigyError as e:
                if e.code in ("UNAUTHORIZED", "FORBIDDEN"):
                    raise
            except (OSError, ConnectionError):
                pass
            await asyncio.sleep(delay)
            delay = min(delay * 2, _MAX_BACKOFF)

    async def observe(self, descriptor, *, key=None, backfill=False,
                      backfill_limit=1000):
        """One-call live primitive: subscribe + SSE + reconnect (+ optional
        /history backfill across gaps). The server tears down subscription
        state on SSE close, so the descriptor is re-registered before each
        session. Closing the generator unsubscribes the descriptor."""
        normalized = normalize_descriptor(descriptor)
        key = key or descriptor_key(normalized)
        last_seen_ts = None
        delay = _INITIAL_BACKOFF
        try:
            while True:
                try:
                    self._data_subs[key] = normalized
                    await self._flush_subscriptions()
                    token = await self._token()
                    reader, writer = await _open_sse(self._endpoint, token,
                                                     None, self._timeout)
                    try:
                        async for frame in _sse_frames(reader):
                            payload = {k: v for k, v in frame.items()
                                       if k not in ("_sse_event", "_sse_id")}
                            t = payload.get("type")
                            # only slash-typed channel events; skip heartbeats
                            if not isinstance(t, str) or "/" not in t:
                                continue
                            # defense-in-depth: parallel observers on one
                            # client must not cross-deliver
                            if not _event_matches_descriptor(payload, normalized):
                                continue
                            if payload.get("ts"):
                                last_seen_ts = payload["ts"]
                            delay = _INITIAL_BACKOFF
                            yield payload
                    finally:
                        writer.close()
                except SynthigyError as e:
                    if e.code in ("UNAUTHORIZED", "FORBIDDEN"):
                        raise

                if backfill and last_seen_ts:
                    try:
                        events = await self.history.events(
                            between=[last_seen_ts, now_iso()],
                            track="entity", limit=backfill_limit)
                        for folded in fold_history_events(
                                events, normalized["records"]):
                            if folded.get("ts"):
                                last_seen_ts = folded["ts"]
                            yield folded
                    except SynthigyError:
                        pass  # best-effort by contract
                await asyncio.sleep(delay)
                delay = min(delay * 2, _MAX_BACKOFF)
        finally:
            # Runs on aclose() OR cancellation. The mirror pop is
            # unconditional (no awaits); the unsubscribe POST is a detached
            # task we TRY to await — from a live aclose() that makes the
            # cleanup deterministic, from a cancelled scope the await
            # raises immediately but the detached task still completes.
            if self._data_subs.pop(key, None) is not None:
                flush = asyncio.ensure_future(self._flush_subscriptions_safe())
                try:
                    await flush
                except (asyncio.CancelledError, Exception):
                    pass

    # ── watch factories ────────────────────────────────────────────────

    def watch(self, interest, *, backpressure="coalesce", buffer_size=100):
        """Raw live subscription: `async with client.watch({...}) as w:
        async for ev in w`. interest = {records?, entities?, relations?,
        relation_xids?, ops?}. Live interest via w.add/remove/set_interest;
        w.mute_request suppresses echoes."""
        return AsyncWatchHandle(self, interest, backpressure=backpressure,
                                buffer_size=buffer_size)

    def watch_schema(self):
        """Stream of {"type": "schema/changed"} deploy events."""
        return AsyncSchemaWatch(self)

    def watch_query(self, entity, args=None, selection=None, *, acting_as=None):
        """Live result-set: snapshot + notify-then-refetch diffing into
        query/added|changed|removed events."""
        return AsyncQueryWatch(self, "search",
                               {"entity": entity, "args": args,
                                "selection": selection},
                               acting_as=acting_as)

    def watch_query_xsql(self, xsql, params=None, *, entity=None, acting_as=None):
        """XSQL variant of watch_query. entity= (kebab-case root) is
        REQUIRED — the server does not infer it from the XSQL."""
        if not entity:
            raise SynthigyError(
                "watch_query_xsql requires entity= (kebab-case root)",
                "INVALID_INTEREST")
        return AsyncQueryWatch(self, "xsql",
                               {"xsql": xsql, "params": params, "entity": entity},
                               acting_as=acting_as)

    def watch_sql_template(self, template, params=None, **kw):
        """Live SQL-template result; requires entities= and/or relations=.
        Emits result/changed when the re-run result differs."""
        return AsyncSqlTemplateWatch(self, template, params, **kw)

    def watch_entities(self, *entities):
        """Entity-touch poke channel — see AsyncEntityWatch."""
        return AsyncEntityWatch(self, entities)

    async def close(self):
        """Tear down live watches + SSE and drop pooled connections."""
        await self._mux.close()
        await self._pool.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()
