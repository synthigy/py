"""Blocking Synthigy client — a thin facade over the async engine.

`synthigy.Client` for scripts, seeds, notebooks and cron jobs: the same
API the SDK always had, driven by ONE background event-loop thread total
(not one per call, not one per watch) running the AsyncClient engine from
async_client.py. N open watches cost ~1 extra thread — the loop.

The loop thread starts LAZILY on the first actual call, never in
__init__: uvicorn/gunicorn spawn workers via fork/spawn and a pre-fork
thread is lost silently. (keep_alive=True opts into eager start — pinning
the SSE session is the point of that flag.) close() tears everything
down; a later call transparently restarts, matching the old client's
"close drops connections, object stays usable" behavior.

Async-first apps (BFFs, FastAPI services) should use the aconnect/
AsyncClient surface directly instead — calling this facade from a
coroutine would block the loop (and calling it from ITS OWN loop thread
is refused: that would deadlock).
"""

import asyncio
import concurrent.futures
import os
import threading

from .async_client import AsyncClient
from .errors import SynthigyError


async def _invoke(fn, *a, **kw):
    """Run a plain sync function ON the loop (interest mutations etc. touch
    asyncio primitives, so they must execute on the loop thread)."""
    return fn(*a, **kw)


class Client:
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
        acting_as    : default impersonation, overridable per call
        key_format   : "kebab" | "snake" | "camel"; None = server default (snake)
        timeout      : per-request timeout in SECONDS
        keep_alive   : pin the SSE session open across watch churn
                       (starts the loop thread eagerly)
        """
        # Validate credentials eagerly (the engine constructs lazily) — same
        # source order as AsyncClient (PLAN-EXEC-IDENTITY.md step 3), just
        # checked here without actually spending a supervised stdio round
        # trip: only fail fast when NO source is even possible.
        if (token is None and not (client_id and client_secret)
                and not os.environ.get("SYNTHIGY_TOKEN")
                and os.environ.get("SYNTHIGY_SUPERVISED") != "1"):
            from .auth import no_token_error
            raise no_token_error()
        self._config = dict(endpoint=endpoint, client_id=client_id,
                            client_secret=client_secret, token=token,
                            scope=scope, audience=audience, acting_as=acting_as,
                            key_format=key_format, timeout=timeout,
                            keep_alive=keep_alive, on_op=on_op)
        self._timeout = timeout
        self._loop = None
        self._thread = None
        self._client = None
        self._start_lock = threading.Lock()
        if keep_alive:
            self._ensure_started()

    # ── loop plumbing ──────────────────────────────────────────────────

    def _ensure_started(self):
        if self._client is not None:
            return self._client
        with self._start_lock:
            if self._client is None:
                loop = asyncio.new_event_loop()
                thread = threading.Thread(target=loop.run_forever,
                                          daemon=True, name="synthigy-loop")
                thread.start()
                self._loop = loop
                self._thread = thread
                # Construct the engine ON the loop so its asyncio
                # primitives (locks, events, tasks) bind to it.
                self._client = asyncio.run_coroutine_threadsafe(
                    _invoke(AsyncClient, **self._config), loop).result()
            return self._client

    def _await(self, coro, timeout=None):
        if threading.current_thread() is self._thread:
            raise SynthigyError(
                "blocking Client called from its own event-loop thread — "
                "use the AsyncClient/aconnect surface in async code",
                "INVALID_CONTEXT")
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    def _call(self, method, *a, **kw):
        c = self._ensure_started()
        return self._await(getattr(c, method)(*a, **kw))

    def _on_loop(self, fn, *a, **kw):
        return self._await(_invoke(fn, *a, **kw))

    def _iter(self, agen):
        """Bridge an async generator into a blocking one. Closing the
        blocking generator runs aclose() on the loop, so cleanup (e.g.
        observe()'s unsubscribe POST) is deterministic."""
        try:
            while True:
                try:
                    yield self._await(agen.__anext__())
                except StopAsyncIteration:
                    return
        finally:
            try:
                asyncio.run_coroutine_threadsafe(
                    agen.aclose(), self._loop).result(5)
            except Exception:
                pass

    def close(self):
        """Tear down live watches + SSE, drop pooled connections, stop the
        loop thread. The object stays usable — a later call restarts."""
        with self._start_lock:
            client, loop, thread = self._client, self._loop, self._thread
            self._client = self._loop = self._thread = None
        if client is None:
            return
        try:
            asyncio.run_coroutine_threadsafe(client.close(), loop).result(10)
        except Exception:
            pass
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()  # safe: run_forever() has returned, thread is joined

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ── verbs ──────────────────────────────────────────────────────────

    def search(self, *a, **kw):
        return self._call("search", *a, **kw)

    def get(self, *a, **kw):
        return self._call("get", *a, **kw)

    def query(self, *a, **kw):
        return self._call("query", *a, **kw)

    def sql_template(self, *a, **kw):
        return self._call("sql_template", *a, **kw)

    def sync(self, *a, **kw):
        return self._call("sync", *a, **kw)

    def stack(self, *a, **kw):
        return self._call("stack", *a, **kw)

    def slice(self, *a, **kw):
        return self._call("slice", *a, **kw)

    def delete(self, *a, **kw):
        return self._call("delete", *a, **kw)

    def purge(self, *a, **kw):
        return self._call("purge", *a, **kw)

    def exec_(self, *a, **kw):
        return self._call("exec_", *a, **kw)

    def search_tree(self, *a, **kw):
        return self._call("search_tree", *a, **kw)

    def get_tree(self, *a, **kw):
        return self._call("get_tree", *a, **kw)

    def deploy(self, *a, **kw):
        return self._call("deploy", *a, **kw)

    def destroy(self, *a, **kw):
        return self._call("destroy", *a, **kw)

    def deployed_model(self, **kw):
        return self._call("deployed_model", **kw)

    def runtime_model(self, **kw):
        return self._call("runtime_model", **kw)

    def schema(self, entities=None):
        return self._call("schema", entities)

    def lint(self, source, *, entity=None, op=None):
        return self._call("lint", source, entity=entity, op=op)

    def onboard(self, xid, *, reset=None, methods=None, ttl_seconds=None,
                return_url=None):
        return self._call("onboard", xid, reset=reset, methods=methods,
                          ttl_seconds=ttl_seconds, return_url=return_url)

    def onboard_complete(self, ticket):
        return self._call("onboard_complete", ticket)

    def token(self, audience=None):
        return self._call("token", audience)

    @property
    def history(self):
        """Temporal query API (POST /history): get_at / events / diff /
        timeline / since."""
        if not hasattr(self, "_history_api"):
            self._history_api = HistoryAPI(self)
        return self._history_api

    # ── subscriptions (advanced — prefer the watch family) ─────────────

    def subscribe(self, *a, **kw):
        return self._call("subscribe", *a, **kw)

    def unsubscribe(self, *a, **kw):
        return self._call("unsubscribe", *a, **kw)

    def subscribe_model(self, **kw):
        return self._call("subscribe_model", **kw)

    def unsubscribe_model(self, **kw):
        return self._call("unsubscribe_model", **kw)

    def set_subscriptions(self, items):
        return self._call("set_subscriptions", items)

    def clear_subscriptions(self):
        return self._call("clear_subscriptions")

    def subscriptions(self):
        return self._call("subscriptions")

    # ── raw streaming ──────────────────────────────────────────────────

    def listen(self):
        """Blocking generator over raw SSE envelopes (see engine listen())."""
        c = self._ensure_started()
        return self._iter(c.listen())

    def observe(self, descriptor, **kw):
        """Blocking generator over the one-call live primitive. Closing the
        generator unsubscribes the descriptor."""
        c = self._ensure_started()
        return self._iter(c.observe(descriptor, **kw))

    # ── live data (watch layer) ────────────────────────────────────────

    def watch(self, interest, **opts):
        """Live subscription. interest = {records?, entities?, relations?,
        relation_xids?, ops?}. Iterate handle.events(); close() when done."""
        c = self._ensure_started()
        handle = c.watch(interest, **opts)
        self._await(handle.register())
        return WatchHandle(self, handle)

    def watch_schema(self):
        """Stream of {"type": "schema/changed"} deploy events."""
        c = self._ensure_started()
        return SchemaWatch(self, c.watch_schema())

    def watch_query(self, entity, args=None, selection=None, **opts):
        """Live result-set: snapshot + notify-then-refetch diffing into
        query/added|changed|removed events."""
        c = self._ensure_started()
        return QueryWatch(self, c.watch_query(entity, args, selection, **opts))

    def watch_query_xsql(self, xsql, params=None, *, entity=None, **opts):
        """XSQL variant of watch_query. entity= (kebab-case root) is
        REQUIRED — the server does not infer it from the XSQL."""
        c = self._ensure_started()
        return QueryWatch(self, c.watch_query_xsql(xsql, params, entity=entity,
                                                   **opts))

    def watch_sql_template(self, template, params=None, **opts):
        """Live SQL-template result; requires entities= and/or relations=.
        Emits result/changed when the re-run result differs."""
        c = self._ensure_started()
        return SqlTemplateWatch(self, c.watch_sql_template(template, params,
                                                           **opts))

    def watch_entities(self, *entities):
        """Entity-touch poke channel — raw entity/touched events."""
        c = self._ensure_started()
        return EntityWatch(self, c.watch_entities(*entities))


class HistoryAPI:
    def __init__(self, facade):
        self._facade = facade

    def _call(self, method, *a, **kw):
        c = self._facade._ensure_started()
        return self._facade._await(getattr(c.history, method)(*a, **kw))

    def get_at(self, *a, **kw):
        return self._call("get_at", *a, **kw)

    def events(self, *a, **kw):
        return self._call("events", *a, **kw)

    def diff(self, *a, **kw):
        return self._call("diff", *a, **kw)

    def timeline(self, *a, **kw):
        return self._call("timeline", *a, **kw)

    def since(self, *a, **kw):
        return self._call("since", *a, **kw)


class _BootstrappedWatch:
    """Shared facade-watch machinery: background bootstrap on the loop,
    blocking ready(), a blocking events() generator over the async
    iterator, close(). events() here draws from ONE underlying stream —
    used for the DERIVED-event watches (QueryWatch, SqlTemplateWatch,
    SchemaWatch), matching the old thread core's behavior for those types
    (their sync equivalents also shared one buffer across events() calls).
    WatchHandle/EntityWatch override events() via _FanOutMixin below —
    raw watches DO fan out, one independent stream per call."""

    def __init__(self, facade, aw):
        self._facade = facade
        self._aw = aw
        self._closed = False
        self._ready_fut = asyncio.run_coroutine_threadsafe(
            aw.ready(), facade._loop)

    def ready(self, timeout=None):
        """Block until the snapshot + watch are wired. Raises any captured
        bootstrap error (or TIMEOUT when `timeout` elapses first)."""
        try:
            self._ready_fut.result(timeout)
        except concurrent.futures.TimeoutError:
            raise SynthigyError("watch bootstrap timed out", "TIMEOUT") from None

    @property
    def closed(self):
        return self._closed

    def events(self):
        """Blocking iterator over this watch's events + sentinels."""
        def gen():
            try:
                self._ready_fut.result()
            except Exception as e:
                yield {"type": "subscription/rejected", "reason": str(e),
                       "records": []}
                return
            while True:
                try:
                    yield self._facade._await(self._aw.__anext__())
                except StopAsyncIteration:
                    return
        return gen()

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._ready_fut.cancel()
        try:
            self._facade._await(self._aw.close(), timeout=5)
        except Exception:
            pass


class _FanOutMixin:
    """For watches whose engine object exposes its own .events() (i.e.
    a fresh independent buffer per call — see AsyncWatchHandle.events()):
    override the base events() so every facade call gets its own engine
    sink too. Two consumers on one handle each see the FULL stream —
    no competing for the same event."""

    def events(self):
        def gen():
            try:
                self._ready_fut.result()
            except Exception as e:
                yield {"type": "subscription/rejected", "reason": str(e),
                       "records": []}
                return
            agen = self._facade._on_loop(self._aw.events)
            while True:
                try:
                    yield self._facade._await(agen.__anext__())
                except StopAsyncIteration:
                    return
        return gen()


class WatchHandle(_FanOutMixin, _BootstrappedWatch):
    """Blocking wrapper over AsyncWatchHandle — raw events, live interest,
    mute_request. Registered (not open-waited) at construction, matching
    the old thread core. FAN-OUT: events() called N times gets N
    independent streams (see _FanOutMixin / AsyncWatchHandle.events())."""

    def __init__(self, facade, handle):
        self._facade = facade
        self._aw = handle
        self._closed = False
        self._ready_fut = concurrent.futures.Future()
        self._ready_fut.set_result(None)   # registered by facade.watch()

    @property
    def interest(self):
        return self._aw.interest

    def add(self, xids):
        if not self._closed:
            self._facade._on_loop(self._aw.add, xids)

    def remove(self, xids):
        if not self._closed:
            self._facade._on_loop(self._aw.remove, xids)

    def set_interest(self, interest):
        if not self._closed:
            self._facade._on_loop(self._aw.set_interest, interest)

    def mute_request(self, request_id, ttl=30.0):
        if not self._closed:
            self._facade._on_loop(self._aw.mute_request, request_id, ttl)


class QueryWatch(_BootstrappedWatch):
    """Blocking wrapper over AsyncQueryWatch — live result-set with
    query/added|changed|removed derived events."""

    def initial(self):
        """First page of rows, server order. None until ready."""
        return self._aw.initial()

    @property
    def records(self):
        """Live xid -> record map, updated by refreshes."""
        return self._aw.records

    def list(self):
        """Snapshot the current records as a list."""
        return self._aw.list()

    def refresh(self):
        """Force a fresh server-side re-evaluation."""
        self._facade._await(self._aw.refresh())


class SqlTemplateWatch(_BootstrappedWatch):
    """Blocking wrapper over AsyncSqlTemplateWatch — result/changed when
    the re-run result differs."""

    def value(self):
        """Current SQL result (list of rows)."""
        return self._aw.value()

    def first(self):
        """First row of the current result, or None."""
        v = self._aw.value()
        return v[0] if isinstance(v, list) and v else None


class SchemaWatch(_BootstrappedWatch):
    """Blocking wrapper over AsyncSchemaWatch — schema/changed on deploy."""


class EntityWatch(_FanOutMixin, _BootstrappedWatch):
    """Blocking wrapper over AsyncEntityWatch — raw entity-touch pokes.
    FAN-OUT: events() called N times gets N independent streams."""
