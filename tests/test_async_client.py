"""Tests for the async-native client (synthigy/async_client.py).

Two things to prove, against the SAME real-socket stub server the rest of
the suite uses (StubServer + SseChannel from test_client.py/test_watch.py):

  1. correctness — the async transport/watch round-trips search + delivers
     live events, and the sync facade returns identical results to the real
     synthigy.Client over the same wire.
  2. the actual point — N concurrently open watches cost O(1) threads on
     the async side, vs O(N) on the sync side (each QueryWatch spawns a
     bootstrap + drain thread pair; see watch_query.py's _RefreshingWatch).
"""

import asyncio
import json
import queue
import threading
import time
import unittest

import synthigy.async_client
from synthigy import Client, SynthigyError
from synthigy.async_client import AsyncClient
from synthigy.facade import Client as SyncFacade
from test_client import StubServer, ok_results  # noqa: F401
from test_sse import data_frame, sse_handler
from test_watch import SseChannel


def wait_until(fn, timeout=5.0, step=0.01):
    """Poll fn until it returns a ready value. None and False mean "not
    ready" — an empty list IS a ready value (e.g. the empty set POST).
    Sync/thread-blocking — only safe when the awaited work runs on a
    DIFFERENT thread (e.g. SyncFacade's background loop). Inside a
    coroutine use async_wait_until instead: a blocking sleep here starves
    the very event loop the work you're waiting on needs to run."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        v = fn()
        if v is not None and v is not False:
            return v
        time.sleep(step)
    raise AssertionError(f"condition not met within {timeout}s")


async def async_wait_until(fn, timeout=5.0, step=0.01):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        v = fn()
        if v is not None and v is not False:
            return v
        await asyncio.sleep(step)
    raise AssertionError(f"condition not met within {timeout}s")


SCHEMA = {
    "id-key": "xid",
    "entities": {
        "movie": {"name": "Movie", "xid": "e-movie",
                  "attributes": {"title": "string"}, "relations": {},
                  "xids": {"attributes": {"title": "a-title"}, "relations": {}}},
    },
}


class AsyncSpikeTestCase(unittest.TestCase):
    def setUp(self):
        self.srv = StubServer()
        self.addCleanup(self.srv.close)
        self.chan = SseChannel()
        self.addCleanup(self.chan.stop)
        self.srv.route("/data/events", self.chan.handler())
        self.srv.route("/data/subscription/set", (200, {"ok": True}))
        self.srv.route("/schema", (200, SCHEMA))
        self.srv.route("/data", (200, ok_results([{"xid": "m-1", "title": "A"}])))
        self._backoff = synthigy.async_client._INITIAL_BACKOFF
        synthigy.async_client._INITIAL_BACKOFF = 0.01
        self.addCleanup(
            lambda: setattr(synthigy.async_client, "_INITIAL_BACKOFF",
                            self._backoff))

    def set_bodies(self):
        return [json.loads(r["body"])
                for r in self.srv.requests_to("/data/subscription/set")]

    def wait_set_post(self, pred, timeout=5.0):
        """Sync-blocking — only safe from a SyncFacade test (background
        loop thread does the work while this thread polls)."""
        def check():
            for b in self.set_bodies():
                if pred(b["subscriptions"]):
                    return b["subscriptions"]
            return None
        return wait_until(check, timeout)

    async def await_set_post(self, pred, timeout=5.0):
        """Async-safe version — call from inside an async def run(), NOT
        wait_set_post (which would deadlock: its sync sleep starves the
        event loop the pending flush needs to run on)."""
        def check():
            for b in self.set_bodies():
                if pred(b["subscriptions"]):
                    return b["subscriptions"]
            return None
        return await async_wait_until(check, timeout)


class TestFanOut(AsyncSpikeTestCase):
    """Raw watches FAN OUT: N independent consumers each see the FULL
    event stream, none of them competing for the same delivery — mirrors
    sdk/js watch.js's Watch (fresh CoalesceBuffer per events() access,
    fanned by the mux push) and the pre-flip sync core's WatchHandle."""

    def test_two_events_calls_both_see_every_event_on_async_handle(self):
        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                async with client.watch({"records": ["m-1"]}) as w:
                    stream1 = w.events()
                    stream2 = w.events()
                    self.chan.send({"type": "record/update",
                                    "record-xid": "m-1", "after": {"n": 1}})
                    ev1 = await asyncio.wait_for(stream1.__anext__(), 5)
                    ev2 = await asyncio.wait_for(stream2.__anext__(), 5)
                    # NOT one-each — BOTH independent streams got it.
                    self.assertEqual(ev1["record"], "m-1")
                    self.assertEqual(ev2["record"], "m-1")
                    self.assertEqual(ev1, ev2)
            finally:
                await client.close()
        asyncio.run(run())

    def test_three_events_calls_all_independent_on_entity_watch(self):
        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                async with client.watch_entities("movie") as w:
                    streams = [w.events() for _ in range(3)]
                    self.chan.send({"type": "entity/touched", "entity": "movie",
                                    "ts": "t1"})
                    events = await asyncio.gather(
                        *[asyncio.wait_for(s.__anext__(), 5) for s in streams])
                    self.assertEqual(len(events), 3)
                    self.assertTrue(all(e["entity"] == "movie" for e in events))
            finally:
                await client.close()
        asyncio.run(run())

    def test_facade_watch_handle_fans_out(self):
        c = SyncFacade(self.srv.endpoint, token="tok")
        self.addCleanup(c.close)
        w = c.watch({"records": ["m-1"]})
        self.addCleanup(w.close)
        q1, q2 = queue.Queue(), queue.Queue()
        it1, it2 = w.events(), w.events()
        threading.Thread(target=lambda: q1.put(next(it1)), daemon=True).start()
        threading.Thread(target=lambda: q2.put(next(it2)), daemon=True).start()
        time.sleep(0.1)
        self.chan.send({"type": "record/update", "record-xid": "m-1",
                        "after": {"n": 1}})
        ev1 = q1.get(timeout=5)
        ev2 = q2.get(timeout=5)
        self.assertEqual(ev1["record"], "m-1")
        self.assertEqual(ev2["record"], "m-1")


class TestContextManagers(AsyncSpikeTestCase):
    """AsyncClient/SyncFacade support with/async-with like httpx's clients —
    close() runs on scope exit, including on an exception inside the block."""

    def test_async_client_async_with_closes(self):
        async def run():
            async with AsyncClient(self.srv.endpoint, token="tok") as c:
                self.assertEqual(await c.search("movie", None, None), [
                    {"xid": "m-1", "title": "A"}])
            # closed: the pool is gone, a further call reopens a fresh one
            # rather than erroring — close() just tears down what was open.
            self.assertTrue(c._pool._closed)
        asyncio.run(run())

    def test_async_client_async_with_closes_on_exception(self):
        async def run():
            with self.assertRaises(ValueError):
                async with AsyncClient(self.srv.endpoint, token="tok") as c:
                    raise ValueError("boom")
            self.assertTrue(c._pool._closed)
        asyncio.run(run())

    def test_sync_facade_with_closes(self):
        with SyncFacade(self.srv.endpoint, token="tok") as c:
            self.assertEqual(c.search("movie", None, None), [
                {"xid": "m-1", "title": "A"}])
        self.assertIsNone(c._client)  # torn down by __exit__

    def test_sync_facade_with_closes_on_exception(self):
        with self.assertRaises(ValueError):
            with SyncFacade(self.srv.endpoint, token="tok") as c:
                raise ValueError("boom")
        self.assertIsNone(c._client)


class TestAsyncCorrectness(AsyncSpikeTestCase):
    def test_derived_added_changed_removed(self):
        """Full sync-QueryWatch parity: refetch after a trigger diffs into
        query/added / query/changed (before/after/changed + provenance) /
        query/removed — mirror of watch_query.py's
        test_added_changed_removed."""
        rows2 = [{"xid": "m-1", "title": "A2"}, {"xid": "m-3", "title": "C"}]

        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                async with client.watch_query("movie") as w:
                    self.assertEqual(w.list(), [{"xid": "m-1", "title": "A"}])
                    # refetches from here on see the NEW result set
                    self.srv.route("/data", (200, ok_results(rows2)))
                    self.chan.send({"type": "record/update", "record-xid": "m-1",
                                    "ts": "t1", "txid": "tx-9",
                                    "before": {"title": "A"},
                                    "after": {"title": "A2"}})
                    evs = {}
                    for _ in range(2):
                        ev = await asyncio.wait_for(w.__anext__(), timeout=5)
                        evs[ev["type"]] = ev
                    self.assertEqual(set(evs), {"query/added", "query/changed"})
                    changed = evs["query/changed"]
                    self.assertEqual(changed["record"],
                                     {"xid": "m-1", "title": "A2"})
                    self.assertEqual(changed["changed"], ["title"])
                    self.assertEqual(changed["before"], {"title": "A"})
                    self.assertEqual(changed["after"], {"title": "A2"})
                    self.assertEqual((changed["ts"], changed["txid"]),
                                     ("t1", "tx-9"))
                    self.assertEqual(evs["query/added"]["record"],
                                     {"xid": "m-3", "title": "C"})
                    self.assertEqual(sorted(w.records), ["m-1", "m-3"])
                    # interest re-synced to the new row set on the wire
                    await self.await_set_post(lambda items: any(
                        i.get("type") == "data"
                        and i.get("records") == ["m-1", "m-3"]
                        for i in items))
                    # now drop m-3: removal surfaces as query/removed
                    self.srv.route("/data", (200, ok_results(
                        [{"xid": "m-1", "title": "A2"}])))
                    self.chan.send({"type": "record/delete", "record-xid": "m-3",
                                    "before": {"title": "C"}})
                    ev = await asyncio.wait_for(w.__anext__(), timeout=5)
                    self.assertEqual(ev["type"], "query/removed")
                    self.assertEqual(ev["record"], "m-3")
            finally:
                await client.close()

        asyncio.run(run())

    def test_sync_facade_matches_real_client(self):
        real = Client(self.srv.endpoint, token="tok")
        self.addCleanup(real.close)
        facade = SyncFacade(self.srv.endpoint, token="tok")
        self.addCleanup(facade.close)

        self.assertEqual(real.search("movie", None, None),
                         facade.search("movie", None, None))

        handle = facade.watch_query("movie")
        self.addCleanup(handle.close)
        handle.ready(timeout=5)
        self.assertEqual(handle.list(), [{"xid": "m-1", "title": "A"}])
        self.srv.route("/data", (200, ok_results(
            [{"xid": "m-1", "title": "A2"}])))
        self.chan.send({"type": "record/update", "record-xid": "m-1",
                        "before": {"title": "A"}, "after": {"title": "A2"}})
        events = handle.events()
        ev = next(events)
        self.assertEqual(ev["type"], "query/changed")
        self.assertEqual(ev["record"], {"xid": "m-1", "title": "A2"})

    def test_new_watch_wakes_via_entity_touch(self):
        """Empty-snapshot fallback — mirrors watch_query.py's
        test_empty_snapshot_falls_back_to_entity_touch: a NEW record
        surfaces as query/added via the entity-touch track."""
        self.srv.route("/data", (200, ok_results([])))

        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                async with client.watch_query("movie") as w:
                    self.assertEqual(w.list(), [])
                    await self.await_set_post(lambda items: any(
                        i.get("type") == "entity" and i.get("entities") == ["movie"]
                        for i in items))
                    self.srv.route("/data", (200, ok_results(
                        [{"xid": "m-9", "title": "New"}])))
                    self.chan.send({"type": "entity/touched", "entity": "movie"})
                    ev = await asyncio.wait_for(w.__anext__(), timeout=5)
                    self.assertEqual(ev["type"], "query/added")
                    self.assertEqual(ev["record"], {"xid": "m-9", "title": "New"})
            finally:
                await client.close()

        asyncio.run(run())

    def test_raw_watch_handle_interest_and_mute(self):
        """AsyncWatchHandle parity: raw events, live add(), mute_request."""
        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                async with client.watch({"records": ["m-1"]}) as w:
                    # raw event delivery
                    self.chan.send({"type": "record/update", "record-xid": "m-1",
                                    "before": {"t": 1}, "after": {"t": 2}})
                    ev = await asyncio.wait_for(w.__anext__(), timeout=5)
                    self.assertEqual(ev["type"], "record/update")
                    self.assertEqual(ev["record"], "m-1")
                    # live add(): union re-POSTs with the widened set
                    w.add(["m-2"])
                    await self.await_set_post(lambda items: any(
                        i.get("type") == "data"
                        and i.get("records") == ["m-1", "m-2"]
                        for i in items))
                    self.chan.send({"type": "record/update", "record-xid": "m-2",
                                    "after": {"t": 3}})
                    ev = await asyncio.wait_for(w.__anext__(), timeout=5)
                    self.assertEqual(ev["record"], "m-2")
                    # mute: the next event carrying this request-id is
                    # swallowed; the one after passes
                    w.mute_request("req-7")
                    self.chan.send({"type": "record/update", "record-xid": "m-1",
                                    "request": "req-7", "after": {"t": 4}})
                    self.chan.send({"type": "record/update", "record-xid": "m-1",
                                    "request": "req-8", "after": {"t": 5}})
                    ev = await asyncio.wait_for(w.__anext__(), timeout=5)
                    self.assertEqual(ev["request"], "req-8")
            finally:
                await client.close()

        asyncio.run(run())

    def test_endpoint_long_tail_smoke(self):
        """history/schema/lint/tree round-trip the same wire shapes as the
        sync client."""
        self.srv.route("/history", (200, {"result": {"events": [{"op": "u"}]}}))
        self.srv.route("/lint", (200, {"diagnostics": [{"message": "m"}]}))
        flat = [{"xid": "n-1", "parent": None},
                {"xid": "n-2", "parent": {"xid": "n-1"}}]

        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                r = await client.history.events(record_xid="m-1", limit=5)
                self.assertEqual(r, {"events": [{"op": "u"}]})
                s = await client.schema(entities=["movie"])
                self.assertIn("entities", s)
                d = await client.lint("@search q\nmovie\n  xid")
                self.assertEqual(d, [{"message": "m"}])
                self.srv.route("/data", (200, ok_results(flat)))
                forest = await client.search_tree("node", "parent", {}, {"xid": None})
                self.assertEqual(len(forest), 1)          # composed, not flat
                self.assertEqual(forest[0]["xid"], "n-1")
                self.assertEqual(forest[0]["_children"][0]["xid"], "n-2")
            finally:
                await client.close()

        asyncio.run(run())


class TestWritesAndBatch(AsyncSpikeTestCase):
    """Write verbs + exec_ produce the same wire ops as the sync client."""

    def data_bodies(self):
        return [json.loads(r["body"]) for r in self.srv.requests_to("/data")]

    def test_write_verbs_wire_shape(self):
        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                await client.stack("movie", {"xid": "m-1", "plays": 2},
                                   acting_as="u-9")
                await client.sync("movie", {"xid": "m-1", "title": "T"})
                await client.delete("movie", {"xid": "m-1"})
                await client.slice("movie", {"xid": {"_eq": "m-1"}},
                                   {"actors": {"xid": None}})
            finally:
                await client.close()

        asyncio.run(run())
        ops = [(b["operations"][0]["op"], b.get("acting_as"))
               for b in self.data_bodies()]
        self.assertEqual(ops, [("stack", "u-9"), ("sync", None),
                               ("delete", None), ("slice", None)])
        sliced = self.data_bodies()[3]["operations"][0]
        # slice: relation selection normalized to wire shape, but WITHOUT
        # the LEFT-join read sugar (no args._join on a write op)
        self.assertEqual(sliced["selections"],
                         {"actors": [{"selections": {"xid": None}}]})

    def test_exec_batch_two_ops_one_request(self):
        self.srv.route("/data", (200, {"results": [
            {"ok": True, "data": [{"xid": "m-1"}]},
            {"ok": True, "data": [{"total": 5}]}]}))

        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                results = await client.exec_(
                    [{"op": "search", "entity": "movie", "args": None,
                      "selections": {"xid": None}},
                     {"op": "sql-template", "template": "SELECT 1",
                      "params": []}])
                self.assertEqual(len(results), 2)
                self.assertTrue(all(r["ok"] for r in results))
            finally:
                await client.close()

        asyncio.run(run())
        self.assertEqual(len(self.data_bodies()), 1)  # ONE round trip
        self.assertEqual(len(self.data_bodies()[0]["operations"]), 2)


class TestSse401Retry(AsyncSpikeTestCase):
    def test_sse_401_clears_token_and_retries_once(self):
        """First SSE connect 401s (rotated token) → token cache cleared,
        immediate retry succeeds. Parity with sse.py's contract."""
        self.srv.route("/oauth/token",
                       (200, {"access_token": "tok-1", "expires_in": 3600}),
                       (200, {"access_token": "tok-2", "expires_in": 3600}))
        state = {"n": 0}
        chan_handler = self.chan.handler()

        def gated(h):
            state["n"] += 1
            if state["n"] == 1:
                h.send_response(401)
                h.send_header("Content-Length", "0")
                h.end_headers()
                return
            chan_handler(h)

        self.srv.route("/data/events", gated)

        async def run():
            client = AsyncClient(self.srv.endpoint,
                                 client_id="c", client_secret="s")
            try:
                async with client.watch_query("movie") as w:
                    self.assertEqual(w.list(), [{"xid": "m-1", "title": "A"}])
                    self.srv.route("/data", (200, ok_results(
                        [{"xid": "m-1", "title": "B"}])))
                    self.chan.send({"type": "record/update", "record-xid": "m-1",
                                    "before": {"title": "A"},
                                    "after": {"title": "B"}})
                    ev = await asyncio.wait_for(w.__anext__(), timeout=5)
                    self.assertEqual(ev["type"], "query/changed")
                    self.assertEqual(ev["record"], {"xid": "m-1", "title": "B"})
            finally:
                await client.close()

        asyncio.run(run())
        self.assertEqual(state["n"], 2, "exactly one 401 + one retry")
        # both tokens were minted: cache was actually cleared
        self.assertEqual(len(self.srv.requests_to("/oauth/token")), 2)


class TestSubscriptionConsolidation(AsyncSpikeTestCase):
    """The other missing piece, now ported: the union of every open watch's
    interest gets POSTed to /data/subscription/set, deduped by signature —
    same contract as WatchMultiplexer._flush (JS watch.js / Go watch.go have
    the equivalent). Proves the mux narrows what the SERVER sends, not just
    that N watches share one client-side connection."""

    def test_watch_posts_its_records_union(self):
        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                async with client.watch_query("movie"):
                    await self.await_set_post(lambda items: items == [
                        {"type": "data", "records": ["m-1"]},
                        {"type": "runtime-model"}])
            finally:
                await client.close()

        asyncio.run(run())

    def test_n_watches_same_interest_dedup_to_one_post(self):
        """N watches on an identical result set share ONE union — opening
        #2..#N must NOT trigger additional POSTs (signature dedup)."""
        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            watches = []
            try:
                for _ in range(20):
                    w = client.watch_query("movie")
                    await w.ready()
                    watches.append(w)
                await self.await_set_post(lambda items: items == [
                    {"type": "data", "records": ["m-1"]},
                    {"type": "runtime-model"}])
                # give any wrongly-triggered extra POSTs a chance to land
                await asyncio.sleep(0.1)
                posts = self.set_bodies()
                self.assertEqual(len(posts), 1,
                                 f"20 identical-interest watches should "
                                 f"consolidate to 1 POST, got {len(posts)}")
            finally:
                for w in watches:
                    await w.close()
                await client.close()

        asyncio.run(run())

    def test_last_watch_closing_posts_empty_union(self):
        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            w = client.watch_query("movie")
            try:
                await w.ready()
                await self.await_set_post(lambda items: items == [
                    {"type": "data", "records": ["m-1"]},
                    {"type": "runtime-model"}])
                await w.close()
                await self.await_set_post(lambda items: items == [])
            finally:
                await client.close()

        asyncio.run(run())


class TestConcurrencyCost(AsyncSpikeTestCase):
    """The actual point of the spike: thread cost of N open watches."""

    N = 40

    def test_async_watches_share_one_connection_zero_extra_threads(self):
        baseline = threading.active_count()

        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            watches = []
            try:
                for _ in range(self.N):
                    w = client.watch_query("movie")
                    await w.ready()
                    watches.append(w)

                # every watch queued, ONE background task (the mux's SSE
                # reader) — not a thread per watch.
                mid = threading.active_count()

                self.srv.route("/data", (200, ok_results(
                    [{"xid": "m-1", "title": "Z"}])))
                self.chan.send({"type": "record/update", "record-xid": "m-1",
                                "before": {"title": "A"}, "after": {"title": "Z"}})
                events = await asyncio.gather(
                    *[asyncio.wait_for(w.__anext__(), timeout=10) for w in watches])
                self.assertEqual(len(events), self.N)
                self.assertTrue(all(e["type"] == "query/changed"
                                    and e["record"]["xid"] == "m-1"
                                    for e in events))
                return mid
            finally:
                for w in watches:
                    await w.close()
                await client.close()

        mid = asyncio.run(run())
        # asyncio.run() itself doesn't spawn extra OS threads for the watches
        # — active_count during the run stayed within a couple of the
        # baseline regardless of N.
        self.assertLessEqual(mid - baseline, 3,
                             f"{self.N} async watches should not cost "
                             f"{mid - baseline} threads")

        def one_connection():
            return self.chan.connections == 1 or None
        wait_until(one_connection)
        self.assertEqual(self.chan.connections, 1,
                         "all N watches should share ONE SSE connection")

    def test_facade_watches_cost_one_loop_thread_total(self):
        """THE FLIP's contract, inverted from the old thread core: N
        blocking watches through the facade cost ~1 extra thread total (the
        event loop), not a bootstrap+drain thread pair per watch."""
        baseline = threading.active_count()
        client = Client(self.srv.endpoint, token="tok")
        self.addCleanup(client.close)
        watches = []
        for _ in range(self.N):
            w = client.watch_query("movie")
            w.ready(timeout=5)
            watches.append(w)
        self.addCleanup(lambda: [w.close() for w in watches])

        mid = threading.active_count()
        self.assertLessEqual(mid - baseline, 3,
                             f"{self.N} facade watches should cost ~1 loop "
                             f"thread, got {mid - baseline}")

        def one_connection():
            return self.chan.connections == 1 or None
        wait_until(one_connection)
        self.assertEqual(self.chan.connections, 1,
                         "all N facade watches should share ONE SSE connection")


RELATIONS_SCHEMA = {
    "id-key": "xid",
    "entities": {
        "movie": {"name": "Movie", "xid": "e-movie",
                  "attributes": {"title": "string"},
                  "relations": {"actors": {"to": "actor"}},
                  "xids": {"attributes": {"title": "a-title"},
                           "relations": {"actors": "r-actors"}}},
    },
}


class TestSchemaTracking(AsyncSpikeTestCase):
    def test_watch_schema_stream_and_resolver_refresh(self):
        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                async with client.watch_schema() as sw:
                    # schema-only union: the runtime-model item alone
                    await self.await_set_post(lambda items: items == [
                        {"type": "runtime-model"}])
                    n_schema = len(self.srv.requests_to("/schema"))
                    self.chan.send({"type": "runtime-model"})
                    ev = await asyncio.wait_for(sw.__anext__(), timeout=5)
                    self.assertEqual(ev, {"type": "schema/changed"})
                    # deploy event re-pulled the schema resolver
                    await async_wait_until(
                        lambda: len(self.srv.requests_to("/schema")) > n_schema)
            finally:
                await client.close()

        asyncio.run(run())

    def test_schema_changed_fans_out_to_regular_watches(self):
        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                async with client.watch({"records": ["m-1"]}) as w:
                    self.chan.send({"type": "runtime-model"})
                    ev = await asyncio.wait_for(w.__anext__(), timeout=5)
                    self.assertEqual(ev["type"], "schema/changed")
            finally:
                await client.close()

        asyncio.run(run())

    def test_relation_xids_keep_interest_off_entity_fallback(self):
        """movie HAS a relation here: an empty snapshot must NOT fall back
        to entity-touch — the relation xids keep the interest non-empty
        locally, and the wire union carries neither (runtime-model only)."""
        self.srv.route("/schema", (200, RELATIONS_SCHEMA))
        self.srv.route("/data", (200, ok_results([])))

        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                async with client.watch_query("movie") as w:
                    await self.await_set_post(lambda items: items == [
                        {"type": "runtime-model"}])
                    self.assertNotIn("entities", w._handle.interest)
                    self.assertEqual(w._handle.interest.get("relation_xids"),
                                     ["r-actors"])
            finally:
                await client.close()

        asyncio.run(run())

    def test_entity_fallback_is_kebab(self):
        """Unknown entity, empty snapshot: the entity-touch fallback is
        kebab-cased (sync-core parity)."""
        self.srv.route("/data", (200, ok_results([])))

        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                async with client.watch_query("musicAlbum"):
                    await self.await_set_post(lambda items: any(
                        i.get("type") == "entity"
                        and i.get("entities") == ["music-album"]
                        for i in items))
            finally:
                await client.close()

        asyncio.run(run())


class TestAsyncListenObserve(AsyncSpikeTestCase):
    def test_listen_open_sentinel_and_reconnect_resume(self):
        ev1 = {"type": "record/update", "record-xid": "u-1"}
        ev2 = {"type": "record/update", "record-xid": "u-2"}
        self.srv.route("/data/events", sse_handler(
            [data_frame(ev1, event_id="41")], [data_frame(ev2)]))

        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                got = []
                gen = client.listen()
                async for frame in gen:
                    if frame["type"] != "sse/open":
                        got.append(frame)
                    if len(got) == 2:
                        break
                await gen.aclose()
                self.assertEqual([g["record-xid"] for g in got],
                                 ["u-1", "u-2"])
            finally:
                await client.close()

        asyncio.run(run())
        reqs = self.srv.requests_to("/data/events")
        self.assertEqual(len(reqs), 2)
        h0 = {k.lower(): v for k, v in reqs[0]["headers"].items()}
        h1 = {k.lower(): v for k, v in reqs[1]["headers"].items()}
        self.assertNotIn("last-event-id", h0)
        self.assertEqual(h1["last-event-id"], "41")

    def test_observe_filters_resubscribes_and_cleans_up(self):
        from contextlib import aclosing
        match = {"type": "record/update", "record-xid": "u-1", "ts": "t1"}
        other = {"type": "record/update", "record-xid": "other"}
        heartbeat = {"type": "heartbeat"}  # not slash-typed → skipped
        self.srv.route("/data/events", sse_handler(
            [data_frame(other) + data_frame(heartbeat) + data_frame(match)]))

        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                async with aclosing(client.observe(["u-1", "u-9"])) as events:
                    async for ev in events:
                        self.assertEqual(ev, match)
                        break
                # aclosing ran the cleanup → empty set POSTed
                await self.await_set_post(lambda items: items == [])
            finally:
                await client.close()

        asyncio.run(run())
        bodies = self.set_bodies()
        self.assertEqual(bodies[0]["subscriptions"][0]["records"],
                         ["u-1", "u-9"])


class TestAsyncSubscriptions(AsyncSpikeTestCase):
    def test_subscribe_bodies_and_full_replace(self):
        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                await client.subscribe(["b-2", "a-1"])
                self.assertEqual(self.set_bodies()[-1], {"subscriptions": [
                    {"type": "data", "records": ["a-1", "b-2"]}]})
                await client.set_subscriptions([
                    {"type": "entity", "entities": ["movie"]},
                    {"type": "runtime-model"}])
                items = self.set_bodies()[-1]["subscriptions"]
                self.assertEqual(items[0],
                                 {"type": "entity", "entities": ["movie"]})
                self.assertEqual(items[1], {"type": "runtime-model"})
                n = len(self.set_bodies())
                out = await client.unsubscribe(["zzz"])
                self.assertEqual(out, {"ok": True})
                self.assertEqual(len(self.set_bodies()), n)  # no extra POST
                await client.clear_subscriptions()
                self.assertEqual(self.set_bodies()[-1], {"subscriptions": []})
                with self.assertRaises(SynthigyError) as cm:
                    await client.subscribe("User")   # legacy firehose form
                self.assertEqual(cm.exception.code, "INVALID_SUBSCRIPTION")
            finally:
                await client.close()

        asyncio.run(run())

    def test_status_get(self):
        self.srv.route("/data/subscription/status",
                       (200, {"subscriptions": [{"type": "data"}]}))

        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok")
            try:
                out = await client.subscriptions()
                self.assertEqual(out["subscriptions"], [{"type": "data"}])
            finally:
                await client.close()

        asyncio.run(run())


class TestKeepAlive(AsyncSpikeTestCase):
    def test_keep_alive_pins_sse_and_skips_empty_posts(self):
        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok",
                                 keep_alive=True)
            try:
                await async_wait_until(
                    lambda: self.chan.connections == 1 or None)
                await asyncio.sleep(0.1)
                self.assertEqual(self.set_bodies(), [])  # no empty-union POST
                w = client.watch({"records": ["a-1"]})
                await w.ready()
                await self.await_set_post(lambda items: any(
                    i.get("type") == "data" for i in items))
                await w.close()
                await asyncio.sleep(0.15)
                # keep_alive: no empty POST on last unregister, SSE stays up
                self.assertTrue(all(b["subscriptions"] != []
                                    for b in self.set_bodies()))
                self.assertEqual(self.chan.connections, 1)
            finally:
                await client.close()

        asyncio.run(run())


class TestAuthExtras(AsyncSpikeTestCase):
    def test_scope_and_token_per_audience(self):
        self.srv.route("/oauth/token",
                       (200, {"access_token": "t-default", "expires_in": 3600}),
                       (200, {"access_token": "t-rob", "expires_in": 3600}))

        async def run():
            client = AsyncClient(self.srv.endpoint, client_id="cid",
                                 client_secret="sec", scope="data:read")
            try:
                self.assertEqual(await client.token(), "t-default")
                self.assertEqual(await client.token(audience="robotics"),
                                 "t-rob")
                self.assertEqual(await client.token(), "t-default")  # cached
            finally:
                await client.close()

        asyncio.run(run())
        reqs = self.srv.requests_to("/oauth/token")
        self.assertEqual(len(reqs), 2)
        self.assertIn("scope=data%3Aread", reqs[0]["body"].decode())
        self.assertIn("audience=robotics", reqs[1]["body"].decode())

    def test_per_call_timeout_and_key_format(self):
        def slow(h):
            time.sleep(0.3)
            body = json.dumps(ok_results([])).encode()
            h.send_response(200)
            h.send_header("Content-Type", "application/json")
            h.send_header("Content-Length", str(len(body)))
            h.end_headers()
            h.wfile.write(body)

        self.srv.route("/data", slow)

        async def run():
            client = AsyncClient(self.srv.endpoint, token="tok",
                                 key_format="kebab")
            try:
                with self.assertRaises(SynthigyError) as cm:
                    await client.search("movie", timeout=0.05)
                self.assertEqual(cm.exception.code, "TIMEOUT")
                # without the per-call deadline the same route succeeds,
                # and per-call key_format overrides the client default
                self.assertEqual(await client.search("movie",
                                                     key_format="camel"), [])
            finally:
                await client.close()

        asyncio.run(run())
        body = json.loads(self.srv.requests_to("/data")[-1]["body"])
        self.assertEqual(body["key_format"], "camel")


if __name__ == "__main__":
    unittest.main()
