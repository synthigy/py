"""Subscription set-replace bodies, SSE listen/observe, backfill fold —
exercised through the blocking facade Client. Stub-server based, mirrors
the JS client.test.js coverage."""

import json
import unittest

import synthigy.async_client
from synthigy import Client, SynthigyError
from synthigy.backfill import fold_history_events
from test_client import StubServer, ok_results  # noqa: F401


def sse_handler(*sessions):
    """Each session is a list of raw SSE frame strings; one connection
    consumes one session (last repeats), then the server closes it."""
    state = {"n": 0}

    def handler(h):
        i = min(state["n"], len(sessions) - 1)
        state["n"] += 1
        h.send_response(200)
        h.send_header("Content-Type", "text/event-stream")
        h.end_headers()
        for frame in sessions[i]:
            h.wfile.write(frame.encode())
        h.wfile.flush()

    return handler


def data_frame(payload, event_id=None):
    out = ""
    if event_id is not None:
        out += f"id: {event_id}\n"
    out += "event: data\n"
    out += f"data: {json.dumps(payload)}\n\n"
    return out


class SseTestCase(unittest.TestCase):
    def setUp(self):
        self.srv = StubServer()
        self.addCleanup(self.srv.close)
        # fast reconnects in tests
        self._backoff = synthigy.async_client._INITIAL_BACKOFF
        synthigy.async_client._INITIAL_BACKOFF = 0.01
        self.addCleanup(
            lambda: setattr(synthigy.async_client, "_INITIAL_BACKOFF",
                            self._backoff))

    def client(self):
        c = Client(self.srv.endpoint, token="tok")
        self.addCleanup(c.close)
        return c

    def set_bodies(self):
        return [json.loads(r["body"])
                for r in self.srv.requests_to("/data/subscription/set")]


class TestSubscriptions(SseTestCase):
    def setUp(self):
        super().setUp()
        self.srv.route("/data/subscription/set", (200, {"ok": True}))

    def test_subscribe_body_sorted_records(self):
        self.client().subscribe(["b-2", "a-1"])
        self.assertEqual(self.set_bodies()[-1], {"subscriptions": [
            {"type": "data", "records": ["a-1", "b-2"]}]})

    def test_operations_sorted(self):
        self.client().subscribe({"records": ["x"],
                                 "operations": ["update", "insert"]})
        self.assertEqual(self.set_bodies()[-1]["subscriptions"][0],
                         {"type": "data", "records": ["x"],
                          "operations": ["insert", "update"]})

    def test_full_replace_every_time(self):
        c = self.client()
        c.subscribe(["a"])
        c.subscribe(["b"])
        body = self.set_bodies()[-1]
        self.assertEqual(len(body["subscriptions"]), 2)  # whole set re-sent

    def test_rejects_legacy_entity_string(self):
        with self.assertRaises(SynthigyError) as cm:
            self.client().subscribe("User")
        self.assertEqual(cm.exception.code, "INVALID_SUBSCRIPTION")

    def test_rejects_empty_records(self):
        with self.assertRaises(SynthigyError) as cm:
            self.client().subscribe([])
        self.assertEqual(cm.exception.code, "EMPTY_RECORDS")

    def test_unsubscribe_by_descriptor_and_unknown_noop(self):
        c = self.client()
        c.subscribe(["a", "b"])
        c.unsubscribe(["b", "a"])  # equal-by-hash regardless of order
        self.assertEqual(self.set_bodies()[-1], {"subscriptions": []})
        n = len(self.set_bodies())
        self.assertEqual(c.unsubscribe(["zzz"]), {"ok": True})
        self.assertEqual(len(self.set_bodies()), n)  # no extra POST

    def test_set_subscriptions_mixed_tracks(self):
        self.client().set_subscriptions([
            {"type": "data", "records": ["r-1"]},
            {"type": "entity", "entities": ["movie", "author"]},
            {"type": "relation", "relations": ["Movie.actors"]},
            {"type": "runtime-model"},
        ])
        items = self.set_bodies()[-1]["subscriptions"]
        self.assertEqual(items[0], {"type": "data", "records": ["r-1"]})
        self.assertEqual(items[1],
                         {"type": "entity", "entities": ["author", "movie"]})
        self.assertEqual(items[2],
                         {"type": "relation", "relations": ["Movie.actors"]})
        self.assertEqual(items[3], {"type": "runtime-model"})

    def test_set_subscriptions_unknown_type(self):
        with self.assertRaises(SynthigyError) as cm:
            self.client().set_subscriptions([{"type": "firehose"}])
        self.assertEqual(cm.exception.code, "UNSUPPORTED_TYPE")

    def test_model_subscribe_variants(self):
        c = self.client()
        c.subscribe_model()
        self.assertEqual(self.set_bodies()[-1]["subscriptions"],
                         [{"type": "runtime-model"}])
        c.subscribe_model(raw=True)
        types = {i["type"] for i in self.set_bodies()[-1]["subscriptions"]}
        self.assertEqual(types, {"runtime-model", "deployed-model"})
        c.unsubscribe_model()
        self.assertEqual(self.set_bodies()[-1]["subscriptions"],
                         [{"type": "deployed-model"}])

    def test_clear(self):
        c = self.client()
        c.subscribe(["a"])
        c.clear_subscriptions()
        self.assertEqual(self.set_bodies()[-1], {"subscriptions": []})

    def test_status_get(self):
        self.srv.route("/data/subscription/status",
                       (200, {"subscriptions": [{"type": "data"}]}))
        out = self.client().subscriptions()
        self.assertEqual(out["subscriptions"], [{"type": "data"}])
        req = self.srv.requests_to("/data/subscription/status")[-1]
        self.assertEqual(req["method"], "GET")


class TestListen(SseTestCase):
    def test_open_sentinel_then_verbatim_payloads(self):
        ev = {"type": "record/update", "record-xid": "u-1",
              "after": {"name": "A"}}
        self.srv.route("/data/events", sse_handler([data_frame(ev, 7)]))
        got = []
        for frame in self.client().listen():
            got.append(frame)
            if len(got) == 2:
                break
        self.assertEqual(got[0], {"type": "sse/open"})
        self.assertEqual(got[1], ev)  # SSE metadata stripped, payload verbatim

    def test_reconnect_carries_last_event_id(self):
        ev1 = {"type": "record/update", "record-xid": "u-1"}
        ev2 = {"type": "record/update", "record-xid": "u-2"}
        self.srv.route("/data/events", sse_handler(
            [data_frame(ev1, event_id="41")], [data_frame(ev2)]))
        got = []
        for frame in self.client().listen():
            if frame["type"] != "sse/open":
                got.append(frame)
            if len(got) == 2:
                break
        self.assertEqual([g["record-xid"] for g in got], ["u-1", "u-2"])
        reqs = self.srv.requests_to("/data/events")
        self.assertEqual(len(reqs), 2)
        # urllib title-cases header names — compare case-insensitively
        h0 = {k.lower(): v for k, v in reqs[0]["headers"].items()}
        h1 = {k.lower(): v for k, v in reqs[1]["headers"].items()}
        self.assertNotIn("last-event-id", h0)
        self.assertEqual(h1["last-event-id"], "41")

    def test_forbidden_is_fatal(self):
        self.srv.route("/data/events", (403, {"error": {
            "message": "no", "code": "FORBIDDEN"}}))
        with self.assertRaises(SynthigyError) as cm:
            next(iter(self.client().listen()))
        self.assertEqual(cm.exception.code, "FORBIDDEN")

    def test_malformed_frames_skipped(self):
        ev = {"type": "entity/touched", "entity": "movie"}
        self.srv.route("/data/events", sse_handler(
            ["event: data\ndata: {not-json\n\n" + data_frame(ev)]))
        got = []
        for frame in self.client().listen():
            got.append(frame)
            if len(got) == 2:
                break
        self.assertEqual(got[1], ev)


class TestObserve(SseTestCase):
    def test_filters_resubscribes_and_cleans_up(self):
        match = {"type": "record/update", "record-xid": "u-1", "ts": "t1"}
        other = {"type": "record/update", "record-xid": "other"}
        heartbeat = {"type": "heartbeat"}  # not slash-typed → skipped
        self.srv.route("/data/subscription/set", (200, {"ok": True}))
        self.srv.route("/data/events", sse_handler(
            [data_frame(other) + data_frame(heartbeat) + data_frame(match)]))
        c = self.client()
        got = []
        for ev in c.observe(["u-1", "u-9"]):
            got.append(ev)
            break
        self.assertEqual(got, [match])
        bodies = self.set_bodies()
        # register before the session + cleanup unsubscribe on close
        self.assertEqual(bodies[0]["subscriptions"][0]["records"],
                         ["u-1", "u-9"])
        self.assertEqual(bodies[-1], {"subscriptions": []})

    def test_relation_events_match_on_data0(self):
        link = {"type": "relation/link", "data": ["u-1", "g-2"]}
        wrong = {"type": "relation/link", "data": ["g-2", "u-1"]}
        self.srv.route("/data/subscription/set", (200, {"ok": True}))
        self.srv.route("/data/events",
                       sse_handler([data_frame(wrong) + data_frame(link)]))
        for ev in self.client().observe(["u-1"]):
            self.assertEqual(ev, link)
            break


class TestBackfillFold(unittest.TestCase):
    def test_folds_per_record_op_and_filters(self):
        rows = [
            {"record-xid": "u-1", "op": "change", "ts": "t1",
             "attribute-xid": "a-name", "value": "A", "txid": 9},
            {"record-xid": "u-1", "op": "change", "ts": "t2",
             "attribute-xid": "a-age", "value": 30, "txid": 9},
            {"record-xid": "outside", "op": "change", "ts": "t1",
             "attribute-xid": "a-x", "value": 1},
            {"record-xid": "u-2", "op": "delete", "ts": "t3",
             "attribute-xid": "__delete__"},
        ]
        out = fold_history_events(rows, {"u-1", "u-2"})
        self.assertEqual(len(out), 2)
        upd = out[0]
        self.assertEqual(upd["type"], "record/update")
        self.assertEqual(upd["after"], {"a-name": "A", "a-age": 30})
        self.assertEqual(upd["ts"], "t2")  # max ts wins
        self.assertTrue(upd["fromBackfill"])
        dele = out[1]
        self.assertEqual(dele["type"], "record/delete")
        self.assertNotIn("after", dele)


if __name__ == "__main__":
    unittest.main()
