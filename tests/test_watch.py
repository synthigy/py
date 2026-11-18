"""Watch-layer tests — event shaping, interest matching, coalesce buffer
modes, and the multiplexer fusion contract (one SSE + one consolidated
subscription/set POST per client), exercised through the blocking facade
Client (the "sync callers see no change" contract over the async engine)."""

import asyncio
import gc
import json
import queue
import threading
import time
import unittest

import synthigy.async_client
from synthigy import Client, SynthigyError
from synthigy.async_client import _AsyncCoalesceBuffer
from synthigy.events import (
    compute_changed, matches, normalize_interest, shape_event,
)
from test_client import StubServer, ok_results  # noqa: F401
from test_sse import data_frame


SCHEMA = {
    "id-key": "xid",
    "entities": {
        "movie": {"name": "Movie", "xid": "e-movie",
                  "attributes": {"title": "string"},
                  "relations": {"actors": {"to": "actor"}},
                  "xids": {"attributes": {"title": "a-title"},
                           "relations": {"actors": "r-actors"}}},
        "author": {"name": "Author", "xid": "e-author",
                   "attributes": {"name": "string"},
                   "relations": {},
                   "xids": {"attributes": {"name": "a-name"},
                            "relations": {}}},
    },
}


def wait_until(fn, timeout=5.0, step=0.01):
    """Poll fn until it returns a ready value. None and False mean "not
    ready" — an empty list IS a ready value (e.g. the empty set POST)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        v = fn()
        if v is not None and v is not False:
            return v
        time.sleep(step)
    raise AssertionError(f"condition not met within {timeout}s")


def pump(iterator):
    """Drain a blocking event iterator into a Queue from a daemon thread."""
    q = queue.Queue()

    def run():
        for ev in iterator:
            q.put(ev)

    threading.Thread(target=run, daemon=True).start()
    return q


class SseChannel:
    """Programmable /data/events endpoint. The test pushes SSE frame
    strings; each connection drains the shared queue until stop().
    drop() ends the CURRENT connection (forces a client reconnect)."""

    def __init__(self):
        self.frames = queue.Queue()
        self.connections = 0
        self._stopped = False

    def handler(self):
        def handle(h):
            self.connections += 1
            h.send_response(200)
            h.send_header("Content-Type", "text/event-stream")
            h.end_headers()
            h.wfile.flush()
            while not self._stopped:
                try:
                    frame = self.frames.get(timeout=0.05)
                except queue.Empty:
                    continue
                if frame is None:      # drop marker — end this session
                    return
                try:
                    h.wfile.write(frame.encode())
                    h.wfile.flush()
                except OSError:
                    return
        return handle

    def send(self, payload, event_id=None):
        self.frames.put(data_frame(payload, event_id))

    def drop(self):
        self.frames.put(None)

    def stop(self):
        self._stopped = True


class WatchTestCase(unittest.TestCase):
    def setUp(self):
        self.srv = StubServer()
        self.addCleanup(self.srv.close)
        self.chan = SseChannel()
        self.addCleanup(self.chan.stop)
        self.srv.route("/data/events", self.chan.handler())
        self.srv.route("/data/subscription/set", (200, {"ok": True}))
        self.srv.route("/schema", (200, SCHEMA))
        self._backoff = synthigy.async_client._INITIAL_BACKOFF
        synthigy.async_client._INITIAL_BACKOFF = 0.01
        self.addCleanup(
            lambda: setattr(synthigy.async_client, "_INITIAL_BACKOFF",
                            self._backoff))

    def client(self, **kw):
        c = Client(self.srv.endpoint, token="tok", **kw)
        self.addCleanup(c.close)
        return c

    def set_bodies(self):
        return [json.loads(r["body"])
                for r in self.srv.requests_to("/data/subscription/set")]

    def wait_set_post(self, pred, timeout=5.0):
        """Wait for a subscription/set POST whose items satisfy pred."""
        def check():
            for b in self.set_bodies():
                if pred(b["subscriptions"]):
                    return b["subscriptions"]
            return None
        return wait_until(check, timeout)


# ---------------------------------------------------------------------------
# Pure units — shape / matches / normalize
# ---------------------------------------------------------------------------


class TestShapeEvent(unittest.TestCase):
    def test_insert(self):
        ev = shape_event({"type": "record/insert", "record-xid": "r-1",
                          "ts": "t1", "txid": "x1", "actor": "alice",
                          "after": {"name": "A", "release_year": 1999}})
        self.assertEqual(ev["type"], "record/insert")
        self.assertEqual(ev["record"], "r-1")
        self.assertEqual(ev["ts"], "t1")
        self.assertEqual(ev["actor"], "alice")
        # verbatim snake_case keys, no translation
        self.assertEqual(ev["after"], {"name": "A", "release_year": 1999})
        self.assertNotIn("before", ev)
        self.assertEqual(ev["changed"], ["name", "release_year"])  # sorted

    def test_update(self):
        ev = shape_event({"type": "record/update", "record-xid": "r-1",
                          "ts": "t2",
                          "before": {"name": "A", "release_year": 1999},
                          "after": {"name": "B", "release_year": 1999}})
        self.assertEqual(ev["before"], {"name": "A", "release_year": 1999})
        self.assertEqual(ev["after"], {"name": "B", "release_year": 1999})
        self.assertEqual(ev["changed"], ["name"])  # shallow diff

    def test_delete(self):
        ev = shape_event({"type": "record/delete", "record-xid": "r-1",
                          "before": {"b_key": 1, "a_key": 2}})
        self.assertEqual(ev["changed"], ["a_key", "b_key"])  # before keys
        self.assertNotIn("after", ev)

    def test_relation_link(self):
        ev = shape_event({"type": "relation/link", "ts": "t",
                          "data": ["u-1", "g-2"]})
        self.assertEqual(ev["type"], "relation/link")
        self.assertEqual(ev["data"], ["u-1", "g-2"])

    def test_touch_pokes_verbatim(self):
        self.assertEqual(
            shape_event({"type": "entity/touched", "entity": "Movie",
                         "ts": "t"}),
            {"type": "entity/touched", "entity": "Movie", "ts": "t"})
        self.assertEqual(
            shape_event({"type": "relation/touched",
                         "relation": "Movie.actors", "ts": "t"}),
            {"type": "relation/touched", "relation": "Movie.actors",
             "ts": "t"})

    def test_non_slash_ignored(self):
        self.assertIsNone(shape_event({"type": "heartbeat"}))
        self.assertIsNone(shape_event({"no": "type"}))

    def test_compute_changed_none_when_uncomputable(self):
        self.assertIsNone(compute_changed(None, None, "update"))
        self.assertIsNone(compute_changed(None, {"a": 1}, "delete"))


class TestMatches(unittest.TestCase):
    def test_record_xid_filtering(self):
        i = {"records": ["r-1"]}
        self.assertTrue(matches(i, {"type": "record/update",
                                    "record-xid": "r-1"}))
        self.assertFalse(matches(i, {"type": "record/update",
                                     "record-xid": "r-9"}))

    def test_ops_narrowing(self):
        i = {"records": ["r-1"], "ops": ["update"]}
        self.assertTrue(matches(i, {"type": "record/update",
                                    "record-xid": "r-1"}))
        self.assertFalse(matches(i, {"type": "record/insert",
                                     "record-xid": "r-1"}))

    def test_relation_link_data0(self):
        i = {"records": ["u-1"]}
        self.assertTrue(matches(i, {"type": "relation/link",
                                    "data": ["u-1", "g-2"]}))
        self.assertFalse(matches(i, {"type": "relation/link",
                                     "data": ["g-2", "u-1"]}))

    def test_entity_and_relation_touched_by_name(self):
        self.assertTrue(matches({"entities": ["movie"]},
                                {"type": "entity/touched",
                                 "entity": "movie"}))
        self.assertFalse(matches({"entities": ["movie"]},
                                 {"type": "entity/touched",
                                  "entity": "author"}))
        self.assertTrue(matches({"relations": ["Movie.actors"]},
                                {"type": "relation/touched",
                                 "relation": "Movie.actors"}))
        self.assertFalse(matches({"relations": ["Movie.actors"]},
                                 {"type": "relation/touched",
                                  "relation": "Movie.genres"}))

    def test_record_interest_does_not_match_pokes(self):
        i = {"records": ["r-1"]}
        self.assertFalse(matches(i, {"type": "entity/touched",
                                     "entity": "movie"}))
        self.assertFalse(matches(i, {"type": "unknown"}))


class TestNormalizeInterest(unittest.TestCase):
    def test_empty_interest(self):
        for interest in (None, {}, {"records": []}, {"ops": ["update"]}):
            with self.assertRaises(SynthigyError) as cm:
                normalize_interest(interest)
            self.assertEqual(cm.exception.code, "EMPTY_INTEREST")

    def test_non_list_rejected(self):
        with self.assertRaises(SynthigyError) as cm:
            normalize_interest({"records": "r-1"})
        self.assertEqual(cm.exception.code, "INVALID_INTEREST")

    def test_dedupe_and_key_passthrough(self):
        out = normalize_interest({"records": ["a", "a", "b"],
                                  "ops": ["update", "update"]})
        self.assertEqual(out, {"records": ["a", "b"], "ops": ["update"]})

    def test_relation_xids_alone_suffice(self):
        out = normalize_interest({"relation_xids": ["rx-1"]})
        self.assertEqual(out, {"relation_xids": ["rx-1"]})


# ---------------------------------------------------------------------------
# CoalesceBuffer
# ---------------------------------------------------------------------------


class TestCoalesceBuffer(unittest.TestCase):
    """_AsyncCoalesceBuffer — the engine's per-watch buffer. Each test body
    is a coroutine run under asyncio.run (the buffer's pop() awaits)."""

    @staticmethod
    async def pop(b, timeout=1):
        """await pop() with a deadline; None on timeout (empty buffer)."""
        try:
            return await asyncio.wait_for(b.pop(), timeout)
        except asyncio.TimeoutError:
            return None

    def test_coalesce_merges_same_record(self):
        async def run():
            b = _AsyncCoalesceBuffer()
            b.push({"type": "record/update", "record": "r-1",
                    "before": {"n": 1}, "after": {"n": 2}, "changed": ["n"]})
            b.push({"type": "record/update", "record": "r-1",
                    "before": {"n": 2}, "after": {"n": 3, "m": 9},
                    "changed": ["m"]})
            ev = await self.pop(b)
            self.assertEqual(ev["before"], {"n": 1})          # earliest before
            self.assertEqual(ev["after"], {"n": 3, "m": 9})   # latest after
            self.assertEqual(sorted(ev["changed"]), ["m", "n"])  # union
            self.assertIsNone(await self.pop(b, 0.05))        # single event
        asyncio.run(run())

    def test_coalesce_delete_supersedes(self):
        async def run():
            b = _AsyncCoalesceBuffer()
            b.push({"type": "record/update", "record": "r-1",
                    "after": {"n": 1}, "changed": ["n"]})
            b.push({"type": "record/delete", "record": "r-1",
                    "before": {"n": 1}, "changed": ["n"]})
            b.push({"type": "record/update", "record": "r-1",
                    "after": {"n": 2}, "changed": ["n"]})  # delete already wins
            ev = await self.pop(b)
            self.assertEqual(ev["type"], "record/delete")
            self.assertIsNone(await self.pop(b, 0.05))
        asyncio.run(run())

    def test_coalesce_distinct_records_kept(self):
        async def run():
            b = _AsyncCoalesceBuffer()
            b.push({"type": "record/update", "record": "r-1", "after": {}})
            b.push({"type": "record/update", "record": "r-2", "after": {}})
            self.assertEqual((await self.pop(b))["record"], "r-1")
            self.assertEqual((await self.pop(b))["record"], "r-2")
        asyncio.run(run())

    def test_coalesce_overflow_drops_oldest(self):
        async def run():
            b = _AsyncCoalesceBuffer(size=2)
            b.push({"type": "record/update", "record": "r-1", "after": {}})
            b.push({"type": "record/update", "record": "r-2", "after": {}})
            b.push({"type": "record/update", "record": "r-3", "after": {}})
            self.assertEqual((await self.pop(b))["record"], "r-2")
            self.assertEqual((await self.pop(b))["record"], "r-3")
        asyncio.run(run())

    def test_sliding_drops_oldest(self):
        async def run():
            b = _AsyncCoalesceBuffer(mode="sliding", size=2)
            for i in range(3):
                b.push({"type": "entity/touched", "entity": f"e{i}"})
            self.assertEqual((await self.pop(b))["entity"], "e1")
            self.assertEqual((await self.pop(b))["entity"], "e2")
        asyncio.run(run())

    def test_lossless_pauses_once_on_overflow(self):
        async def run():
            b = _AsyncCoalesceBuffer(mode="lossless", size=2)
            for i in range(4):
                b.push({"type": "entity/touched", "entity": f"e{i}"})
            self.assertEqual((await self.pop(b))["entity"], "e0")
            self.assertEqual((await self.pop(b))["entity"], "e1")
            self.assertEqual(await self.pop(b), {"type": "paused"})
            self.assertIsNone(await self.pop(b, 0.05))  # e2/e3 were dropped
            b.push({"type": "entity/touched", "entity": "e9"})  # resumes
            self.assertEqual((await self.pop(b))["entity"], "e9")
        asyncio.run(run())

    def test_lossless_never_coalesces_records(self):
        async def run():
            b = _AsyncCoalesceBuffer(mode="lossless", size=10)
            b.push({"type": "record/update", "record": "r-1", "after": {"n": 1}})
            b.push({"type": "record/update", "record": "r-1", "after": {"n": 2}})
            self.assertEqual((await self.pop(b))["after"], {"n": 1})
            self.assertEqual((await self.pop(b))["after"], {"n": 2})
        asyncio.run(run())

    def test_close_unblocks_and_drains(self):
        async def run():
            b = _AsyncCoalesceBuffer()
            popper = asyncio.ensure_future(b.pop())
            await asyncio.sleep(0.05)
            b.close()
            self.assertIsNone(await asyncio.wait_for(popper, 1))
            # queued events still drain after close
            b2 = _AsyncCoalesceBuffer()
            b2.push({"type": "entity/touched", "entity": "e"})
            b2.close()
            self.assertEqual((await self.pop(b2))["entity"], "e")
            self.assertIsNone(await self.pop(b2, 0.05))
        asyncio.run(run())


# ---------------------------------------------------------------------------
# Multiplexer fusion + end-to-end delivery
# ---------------------------------------------------------------------------


class TestFusion(WatchTestCase):
    def test_two_watches_one_sse_one_union_post_empty_on_close(self):
        c = self.client()
        w1 = c.watch({"records": ["a-1"]})
        w2 = c.watch({"records": ["b-2"]})
        subs = self.wait_set_post(lambda items: any(
            i.get("type") == "data" and i.get("records") == ["a-1", "b-2"]
            for i in items))
        self.assertIn({"type": "runtime-model"}, subs)   # schema wanted
        self.assertEqual(self.chan.connections, 1)       # ONE SSE
        w2.close()
        self.wait_set_post(lambda items: any(
            i.get("type") == "data" and i.get("records") == ["a-1"]
            for i in items))
        w1.close()   # last watch: empty set POSTed, SSE torn down
        self.wait_set_post(lambda items: items == [])

    def test_union_tracks_and_ops(self):
        c = self.client()
        w = c.watch({"records": ["r-1"], "entities": ["movie"],
                     "relations": ["Movie.actors"], "ops": ["update"]})
        subs = self.wait_set_post(lambda items: len(items) == 4)
        self.assertEqual(subs[0], {"type": "data", "records": ["r-1"],
                                   "operations": ["update"]})
        self.assertEqual(subs[1], {"type": "entity", "entities": ["movie"]})
        self.assertEqual(subs[2], {"type": "relation",
                                   "relations": ["Movie.actors"]})
        self.assertEqual(subs[3], {"type": "runtime-model"})
        w.close()

    def test_a_watch_on_every_op_keeps_the_filter_off(self):
        c = self.client()
        w1 = c.watch({"records": ["r-1"], "ops": ["insert"]})
        self.wait_set_post(lambda items: items and items[0].get("operations") == ["insert"])
        w2 = c.watch({"records": ["r-2"]})
        subs = self.wait_set_post(lambda items: items and items[0].get("records") == ["r-1", "r-2"])
        self.assertNotIn("operations", subs[0])
        w1.close(); w2.close()

    def test_add_widens_interest_and_reflushes(self):
        c = self.client()
        w = c.watch({"records": ["a-1"]})
        self.wait_set_post(lambda items: any(
            i.get("records") == ["a-1"] for i in items))
        w.add(["b-2"])
        self.wait_set_post(lambda items: any(
            i.get("records") == ["a-1", "b-2"] for i in items))
        w.remove(["a-1"])
        self.wait_set_post(lambda items: any(
            i.get("records") == ["b-2"] for i in items))
        w.close()

    def test_keep_alive_pins_sse_and_skips_empty_posts(self):
        c = self.client(keep_alive=True)   # eager SSE at construction
        wait_until(lambda: self.chan.connections == 1)
        time.sleep(0.1)
        self.assertEqual(self.set_bodies(), [])   # no empty-union POST
        w = c.watch({"records": ["a-1"]})
        self.wait_set_post(lambda items: any(
            i.get("type") == "data" for i in items))
        w.close()
        time.sleep(0.15)
        # keep_alive: no empty POST on last unregister, SSE stays open
        self.assertTrue(all(b["subscriptions"] != []
                            for b in self.set_bodies()))
        self.assertEqual(self.chan.connections, 1)


class TestEndToEnd(WatchTestCase):
    def test_shaped_delivery_and_foreign_record_filtered(self):
        c = self.client()
        w = c.watch({"records": ["r-1"]})
        q = pump(w.events())
        self.wait_set_post(lambda items: any(
            "r-1" in (i.get("records") or []) for i in items))
        # foreign record first — must NOT be delivered
        self.chan.send({"type": "record/update", "record-xid": "r-9",
                        "ts": "t0", "before": {"name": "X"},
                        "after": {"name": "Y"}})
        self.chan.send({"type": "record/update", "record-xid": "r-1",
                        "ts": "t1", "txid": "tx-1", "actor": "alice",
                        "before": {"name": "A", "release_year": 1999},
                        "after": {"name": "B", "release_year": 1999}})
        ev = q.get(timeout=5)
        self.assertEqual(ev["type"], "record/update")
        self.assertEqual(ev["record"], "r-1")   # r-9 was filtered
        self.assertEqual(ev["before"], {"name": "A", "release_year": 1999})
        self.assertEqual(ev["after"], {"name": "B", "release_year": 1999})
        self.assertEqual(ev["changed"], ["name"])
        self.assertEqual((ev["ts"], ev["txid"], ev["actor"]),
                         ("t1", "tx-1", "alice"))
        w.close()

    def test_reconnect_resumed_sentinel_and_reflush(self):
        c = self.client()
        w = c.watch({"records": ["a-1"]})
        q = pump(w.events())
        self.wait_set_post(lambda items: any(
            i.get("records") == ["a-1"] for i in items))
        n = len(self.set_bodies())
        self.chan.drop()   # server ends the session -> client reconnects
        ev = q.get(timeout=5)
        self.assertEqual(ev["type"], "connection/resumed")
        wait_until(lambda: len(self.set_bodies()) > n)   # re-flushed
        self.assertEqual(
            self.set_bodies()[-1]["subscriptions"][0]["records"], ["a-1"])
        self.assertEqual(self.chan.connections, 2)
        w.close()

    def test_fresh_iterator_per_events_call(self):
        # FAN-OUT, not a shared queue: two events() calls on the SAME
        # handle both see every event — no competing for one delivery.
        c = self.client()
        w = c.watch({"records": ["r-1"]})
        q1 = pump(w.events())
        q2 = pump(w.events())
        self.wait_set_post(lambda items: any(
            "r-1" in (i.get("records") or []) for i in items))
        self.chan.send({"type": "record/update", "record-xid": "r-1",
                        "after": {"n": 1}})
        self.assertEqual(q1.get(timeout=5)["record"], "r-1")
        self.assertEqual(q2.get(timeout=5)["record"], "r-1")
        w.close()



GONE = (400, {"error": {"message": "gone", "code": "STREAM_NOT_FOUND"}})


class TestStreams(WatchTestCase):
    def test_every_post_names_the_open_stream(self):
        c = self.client()
        w = c.watch({"records": ["a-1"]})
        q = pump(w.events())
        self.wait_set_post(lambda items: any(
            i.get("records") == ["a-1"] for i in items))
        self.chan.drop()
        self.assertEqual(q.get(timeout=5)["type"], "connection/resumed")
        wait_until(lambda: len(self.set_bodies()) >= 2)
        streams = [r["path"].partition("?stream=")[2]
                   for r in self.srv.requests_to("/data/events")][:2]
        self.assertEqual([b.get("stream") for b in self.set_bodies()[:2]],
                         streams)
        self.assertNotEqual(streams[0], streams[1])
        w.close()

    def test_a_closed_stream_is_not_a_rejection(self):
        self.srv.route("/data/subscription/set", GONE, (200, {"ok": True}))
        c = self.client()
        w = c.watch({"records": ["a-1"]})
        q = pump(w.events())
        wait_until(lambda: len(self.set_bodies()) == 1)
        time.sleep(0.1)
        self.assertTrue(q.empty())
        self.assertIsNone(w._aw._client._mux._stream_id)
        self.assertFalse(w._aw._subscribed.is_set())
        self.chan.drop()
        self.assertEqual(q.get(timeout=5)["type"], "connection/resumed")
        wait_until(lambda: len(self.set_bodies()) == 2)
        self.assertEqual(self.set_bodies()[1]["subscriptions"][0]["records"],
                         ["a-1"])
        wait_until(lambda: w._aw._subscribed.is_set())
        w.close()


    def test_a_post_that_outlives_its_stream_waits_for_the_next_open(self):
        post_gate, open_gate = threading.Event(), threading.Event()
        sse = self.chan.handler()

        def events(h):
            if self.chan.connections >= 1:
                open_gate.wait(5)
            sse(h)

        def subs(h):
            if len(self.set_bodies()) == 2:
                post_gate.wait(5)
            h._respond(200, {"ok": True})

        self.srv.route("/data/events", events)
        self.srv.route("/data/subscription/set", subs)
        c = self.client()
        w1 = c.watch({"records": ["a-1"]})
        pump(w1.events())
        mux = w1._aw._client._mux
        wait_until(lambda: w1._aw._subscribed.is_set())
        w2 = queue.Queue()
        threading.Thread(target=lambda: w2.put(c.watch({"records": ["b-2"]})),
                         daemon=True).start()
        wait_until(lambda: len(self.set_bodies()) == 2)
        self.chan.drop()
        wait_until(lambda: mux._stream_id is None)
        post_gate.set()
        w2 = w2.get(timeout=5)
        time.sleep(0.2)
        self.assertFalse(w2._aw._subscribed.is_set())
        open_gate.set()
        wait_until(lambda: len(self.set_bodies()) == 3)
        wait_until(lambda: w2._aw._subscribed.is_set())
        w2.close()
        w1.close()

    def test_nothing_is_posted_between_streams(self):
        c = self.client()
        w1 = c.watch({"records": ["a-1"]})
        w2 = c.watch({"records": ["b-2"]})
        self.wait_set_post(lambda items: any(
            i.get("records") == ["a-1", "b-2"] for i in items))
        n = len(self.set_bodies())
        self.srv.route("/data/events", (503, {"error": {"message": "down",
                                                        "code": "HTTP_ERROR"}}))
        self.chan.drop()
        wait_until(lambda: len(self.srv.requests_to("/data/events")) >= 2)
        w2.close()
        time.sleep(0.2)
        self.assertEqual(len(self.set_bodies()), n)
        w1.close()


def _fake_watch(records):
    pushed = []
    h = type("H", (), {})()
    h.interest = {"records": records}
    h._subscribed = asyncio.Event()
    h._push = pushed.append
    h.pushed = pushed
    return h


class _SubsClient:
    def __init__(self, on_post=None):
        self.posts = []
        self.on_post = on_post

    async def _set_subscriptions(self, items, *, stream=None):
        self.posts.append(stream)
        if self.on_post:
            self.on_post(stream)


class TestMuxStreamId(unittest.TestCase):
    def run_mux(self, on_post, stream):
        async def run():
            client = _SubsClient()
            mux = synthigy.async_client.AsyncWatchMultiplexer(client, schema=False)
            client.on_post = on_post and (lambda s: on_post(mux, s))
            mux._stream_id = stream
            h = _fake_watch(["a-1"])
            mux._watches.add(h)
            await mux._flush_safe()
            return mux, client, h
        return asyncio.run(run())

    def test_no_stream_no_post(self):
        mux, client, h = self.run_mux(None, None)
        self.assertEqual(client.posts, [])
        self.assertFalse(h._subscribed.is_set())
        self.assertIsNone(mux._last_union)

    def test_post_names_the_stream_and_records_the_union(self):
        mux, client, h = self.run_mux(None, "s-1")
        self.assertEqual(client.posts, ["s-1"])
        self.assertTrue(h._subscribed.is_set())
        self.assertIsNotNone(mux._last_union)

    def test_refusal_of_the_current_stream_clears_it(self):
        def gone(mux, s):
            raise SynthigyError("gone", "STREAM_NOT_FOUND", status=400)
        mux, client, h = self.run_mux(gone, "s-1")
        self.assertIsNone(mux._stream_id)
        self.assertEqual(h.pushed, [])
        self.assertFalse(h._subscribed.is_set())
        self.assertIsNone(mux._last_union)

    def test_an_old_streams_refusal_keeps_the_new_stream(self):
        def gone(mux, s):
            mux._stream_id = "s-new"
            raise SynthigyError("gone", "STREAM_NOT_FOUND", status=400)
        mux, client, h = self.run_mux(gone, "s-old")
        self.assertEqual(mux._stream_id, "s-new")
        self.assertEqual(h.pushed, [])

    def test_a_union_posted_to_an_old_stream_is_not_recorded(self):
        def moved(mux, s):
            mux._stream_id = "s-new"
        mux, client, h = self.run_mux(moved, "s-old")
        self.assertIsNone(mux._last_union)
        self.assertFalse(h._subscribed.is_set())

    def test_a_post_whose_stream_ended_settles_nothing(self):
        def ended(mux, s):
            mux._stream_id = None
        mux, client, h = self.run_mux(ended, "s-1")
        self.assertEqual(client.posts, ["s-1"])
        self.assertIsNone(mux._last_union)
        self.assertFalse(h._subscribed.is_set())

    def test_an_unchanged_union_without_a_stream_marks_nothing(self):
        async def run():
            client = _SubsClient()
            mux = synthigy.async_client.AsyncWatchMultiplexer(client, schema=False)
            mux._stream_id = "s-1"
            mux._watches.add(_fake_watch(["a-1"]))
            await mux._flush_safe()
            mux._stream_id = None
            late = _fake_watch(["a-1"])
            mux._watches.add(late)
            await mux._flush_safe()
            return client, late
        client, late = asyncio.run(run())
        self.assertEqual(client.posts, ["s-1"])
        self.assertFalse(late._subscribed.is_set())

    def test_an_unchanged_union_after_stream_not_found_marks_nothing(self):
        async def run():
            client = _SubsClient()
            mux = synthigy.async_client.AsyncWatchMultiplexer(client, schema=False)
            mux._stream_id = "s-1"
            h = _fake_watch(["a-1"])
            mux._watches.add(h)
            await mux._flush_safe()

            def gone(stream):
                raise SynthigyError("gone", "STREAM_NOT_FOUND", status=400)
            client.on_post = gone
            h.interest = {"records": ["a-1", "b-2"]}
            await mux._flush_safe()
            h.interest = {"records": ["a-1"]}
            late = _fake_watch(["a-1"])
            mux._watches.add(late)
            await mux._flush_safe()
            return client, late
        client, late = asyncio.run(run())
        self.assertEqual(client.posts, ["s-1", "s-1"])
        self.assertFalse(late._subscribed.is_set())


async def _drain(it):
    return [ev async for ev in it]


class TestFatal(WatchTestCase):
    def test_a_refusal_during_a_replay_still_reaches_the_watch(self):
        async def run():
            mux = synthigy.async_client.AsyncWatchMultiplexer(_SubsClient(),
                                                               schema=False)
            h = synthigy.async_client.AsyncWatchHandle(None, {"records": ["r-1"]})
            h._replaying = []
            h._replay_task = asyncio.ensure_future(asyncio.sleep(10))
            mux._watches.add(h)
            mux._fatal(SynthigyError("no", "FORBIDDEN", status=403))
            return [ev async for ev in h]
        got = asyncio.run(run())
        self.assertEqual([e["type"] for e in got], ["subscription/rejected"])
        self.assertEqual(got[0]["error"].code, "FORBIDDEN")

    def test_events_opened_after_a_refusal_get_it_and_end(self):
        async def run():
            mux = synthigy.async_client.AsyncWatchMultiplexer(_SubsClient(),
                                                               schema=False)
            h = synthigy.async_client.AsyncWatchHandle(None, {"records": ["r-1"]})
            mux._watches.add(h)
            mux._fatal(SynthigyError("no", "FORBIDDEN", status=403))
            return await asyncio.wait_for(
                asyncio.gather(*([_drain(h.events()) for _ in range(2)])), 2)
        got = asyncio.run(run())
        self.assertEqual([[e["type"] for e in evs] for evs in got],
                         [["subscription/rejected"]] * 2)

    def test_a_watch_after_a_refused_stream_opens_a_new_one(self):
        sse = self.chan.handler()
        calls = []

        def events(h):
            calls.append(1)
            if len(calls) == 1:
                h._respond(403, {"error": REJECTION})
            else:
                sse(h)
        self.srv.route("/data/events", events)
        c = self.client()
        w1 = c.watch({"records": ["a-1"]})
        q = pump(w1.events())
        self.assertEqual(q.get(timeout=5)["type"], "subscription/rejected")
        w2 = c.watch({"records": ["b-2"]})
        self.wait_set_post(lambda items: any(
            "b-2" in i.get("records", []) for i in items))
        self.assertEqual(len(calls), 2)
        w2.close()

    def test_a_watch_after_a_refusal_posts_only_its_own_interest(self):
        sse = self.chan.handler()
        calls = []

        def events(h):
            calls.append(1)
            if len(calls) == 1:
                h._respond(403, {"error": REJECTION})
            else:
                sse(h)
        self.srv.route("/data/events", events)
        c = self.client()
        w1 = c.watch({"records": ["a-1"]})
        q = pump(w1.events())
        self.assertEqual(q.get(timeout=5)["type"], "subscription/rejected")
        w2 = c.watch({"records": ["b-2"]})
        items = self.wait_set_post(lambda items: any(
            "b-2" in i.get("records", []) for i in items))
        self.assertEqual([i.get("records") for i in items
                          if i.get("type") == "data"], [["b-2"]])
        w1.close()
        w2.close()

    def test_fatal_drops_ended_watches_and_a_later_close_is_harmless(self):
        async def run():
            mux = synthigy.async_client.AsyncWatchMultiplexer(_SubsClient(),
                                                               schema=False)
            client = type("C", (), {"_mux": mux})()
            h = synthigy.async_client.AsyncWatchHandle(client, {"records": ["r-1"]})
            h._registered = True
            sw = synthigy.async_client.AsyncSchemaWatch(client)
            mux._watches.add(h)
            mux._schema_watches.add(sw)
            mux._fatal(SynthigyError("no", "FORBIDDEN", status=403))
            await h.close()
            await sw.close()
            return mux
        mux = asyncio.run(run())
        self.assertEqual(mux._watches, set())
        self.assertEqual(mux._schema_watches, set())
        self.assertIsNone(mux._flush_pending)

    def test_a_replaced_refused_task_logs_no_unretrieved_exception(self):
        class Refused(_SubsClient):
            _endpoint = "http://unused"
            _timeout = 1
            _token_manager = None

            async def _token(self):
                raise SynthigyError("no", "FORBIDDEN", status=403)

        async def run():
            seen = []
            asyncio.get_running_loop().set_exception_handler(
                lambda loop, ctx: seen.append(ctx.get("message")))
            mux = synthigy.async_client.AsyncWatchMultiplexer(Refused(),
                                                               schema=False)
            mux._ensure_task()
            await asyncio.wait([mux._task])
            mux._ensure_task()
            await asyncio.wait([mux._task])
            mux._task.exception()
            mux._task = None
            gc.collect()
            await asyncio.sleep(0)
            return seen
        seen = asyncio.run(run())
        self.assertFalse([m for m in seen
                          if m and "never retrieved" in m], seen)


class TestBackfill(WatchTestCase):
    def history(self, rows, delay=0.0, status=200):
        def handle(h):
            body = json.loads(h.request_body)
            time.sleep(delay)
            xid = body["opts"].get("record-xid")
            h._respond(status, {"result": [r for r in rows
                                           if r["record-xid"] == xid]})
        self.srv.route("/history", handle)

    def watch_until_gap(self, live_after_drop=None):
        c = self.client()
        w = c.watch({"records": ["r-1", "r-2"], "ops": ["update"]},
                    backfill=True, backpressure="lossless")
        q = pump(w.events())
        self.wait_set_post(lambda items: any(
            i.get("records") == ["r-1", "r-2"] for i in items))
        self.chan.send({"type": "record/update", "record-xid": "r-1",
                        "ts": "t1", "after": {"title": "A"}})
        self.assertEqual(q.get(timeout=5)["ts"], "t1")
        self.chan.drop()
        if live_after_drop:
            self.chan.send(live_after_drop)
        self.addCleanup(w.close)
        return q

    ROWS = [
        {"record-xid": "r-1", "op": "change", "ts": "t1",
         "attribute-xid": "a-title", "value": "A"},
        {"record-xid": "r-1", "op": "change", "ts": "t2",
         "attribute-xid": "a-title", "value": "B"},
        {"record-xid": "r-2", "op": "insert", "ts": "t2",
         "attribute-xid": "a-title", "value": "N"},
        {"record-xid": "r-2", "op": "change", "ts": "t3",
         "attribute-xid": "a-title", "value": "M"},
    ]

    def test_replays_the_gap_per_record_before_the_sentinel(self):
        self.history(self.ROWS)
        q = self.watch_until_gap()
        got = [q.get(timeout=5) for _ in range(3)]
        self.assertEqual([(e["type"], e.get("record"), e.get("ts")) for e in got],
                         [("record/update", "r-1", "t2"),
                          ("record/update", "r-2", "t3"),
                          ("connection/resumed", None, None)])
        self.assertEqual(got[0]["after"], {"title": "B"})
        self.assertTrue(got[0]["fromBackfill"])
        opts = [json.loads(r["body"])["opts"]
                for r in self.srv.requests_to("/history")]
        self.assertEqual(sorted(o["record-xid"] for o in opts), ["r-1", "r-2"])
        self.assertTrue(all(o["between"][0] == "t1" and o["track"] == "entity"
                            and o["limit"] == 1000 for o in opts))

    def test_live_events_during_the_replay_come_after_it(self):
        self.history(self.ROWS, delay=0.3)
        q = self.watch_until_gap({"type": "record/update", "record-xid": "r-1",
                                  "ts": "t4", "after": {"title": "L"}})
        got = [q.get(timeout=5) for _ in range(4)]
        self.assertEqual([(e["type"], e.get("ts")) for e in got],
                         [("record/update", "t2"), ("record/update", "t3"),
                          ("connection/resumed", None), ("record/update", "t4")])
        self.assertNotIn("fromBackfill", got[3])

    def test_a_failed_history_still_delivers_the_sentinel_and_held_events(self):
        self.history([], delay=0.3, status=500)
        q = self.watch_until_gap({"type": "record/update", "record-xid": "r-1",
                                  "ts": "t4", "after": {"title": "L"}})
        got = [q.get(timeout=5) for _ in range(2)]
        self.assertEqual([(e["type"], e.get("ts")) for e in got],
                         [("connection/resumed", None), ("record/update", "t4")])


REJECTION = {"message": "no", "code": "FORBIDDEN", "hint": "h",
             "rule": "stamp", "doc": "errors/stamp"}


class TestRejectedCarriesTheError(WatchTestCase):
    def assert_carries(self, ev):
        self.assertEqual(ev["type"], "subscription/rejected")
        e = ev["error"]
        self.assertIsInstance(e, SynthigyError)
        self.assertEqual(ev["reason"], str(e))
        self.assertEqual((e.code, e.hint, e.rule, e.doc),
                         ("FORBIDDEN", "h", "stamp", "errors/stamp"))

    def test_refused_subscribe(self):
        self.srv.route("/data/subscription/set", (403, {"error": REJECTION}))
        w = self.client().watch({"records": ["r-1"]})
        q = pump(w.events())
        self.assert_carries(q.get(timeout=5))
        w.close()

    def test_refused_query_watch(self):
        self.srv.route("/data", (200, {"results": [{"ok": False, "error": REJECTION}]}))
        qw = self.client().watch_query("movie", {}, {"title": None})
        self.addCleanup(qw.close)
        q = pump(qw.events())
        self.assert_carries(q.get(timeout=5))


if __name__ == "__main__":
    unittest.main()
