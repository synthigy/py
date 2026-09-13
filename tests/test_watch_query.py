"""Phase 3 tests: QueryWatch + SqlTemplateWatch — snapshot, coalesced
notify-then-refetch refresh, diff events, interest re-sync."""

import json
import time
import unittest

import synthigy
from synthigy import SynthigyError
from test_client import ok_results
from test_watch import WatchTestCase, pump, wait_until


class TestQueryWatch(WatchTestCase):
    def test_added_changed_removed(self):
        rows1 = [{"xid": "m-1", "title": "A"}, {"xid": "m-2", "title": "B"}]
        rows2 = [{"xid": "m-1", "title": "A2"}, {"xid": "m-3", "title": "C"}]
        self.srv.route("/data",
                       (200, ok_results(rows1)),
                       (200, ok_results(rows2)))
        c = self.client()
        qw = c.watch_query("movie", {}, {"title": None})
        self.addCleanup(qw.close)
        qw.ready(timeout=5)
        self.assertEqual(qw.initial(), rows1)
        self.assertEqual(sorted(qw.records), ["m-1", "m-2"])
        q = pump(qw.events())
        # snapshot xids on the wire (relation_xids stay local)
        self.wait_set_post(lambda items: any(
            i.get("type") == "data" and i.get("records") == ["m-1", "m-2"]
            for i in items))
        self.chan.send({"type": "record/update", "record-xid": "m-1",
                        "ts": "t1", "txid": "tx-9",
                        "before": {"title": "A"}, "after": {"title": "A2"}})
        evs = {}
        for _ in range(3):
            ev = q.get(timeout=5)
            evs[ev["type"]] = ev
        self.assertEqual(set(evs), {"query/added", "query/changed",
                                    "query/removed"})
        changed = evs["query/changed"]
        self.assertEqual(changed["record"], {"xid": "m-1", "title": "A2"})
        self.assertEqual(changed["changed"], ["title"])
        self.assertEqual(changed["before"], {"title": "A"})
        self.assertEqual(changed["after"], {"title": "A2"})
        self.assertEqual((changed["ts"], changed["txid"]), ("t1", "tx-9"))
        self.assertEqual(evs["query/added"]["record"],
                         {"xid": "m-3", "title": "C"})
        self.assertEqual(evs["query/removed"]["record"], "m-2")
        self.assertEqual(sorted(qw.records), ["m-1", "m-3"])
        self.assertEqual(len(qw.list()), 2)
        # interest re-synced to the new row set
        self.wait_set_post(lambda items: any(
            i.get("type") == "data" and i.get("records") == ["m-1", "m-3"]
            for i in items))

    def test_nested_xids_watched(self):
        rows = [{"xid": "m-1", "title": "A",
                 "actors": [{"xid": "a-9", "name": "Z"}]}]
        self.srv.route("/data", (200, ok_results(rows)))
        c = self.client()
        qw = c.watch_query("movie", {}, {"title": None,
                                         "actors": {"name": None}})
        self.addCleanup(qw.close)
        qw.ready(timeout=5)
        self.wait_set_post(lambda items: any(
            i.get("type") == "data" and i.get("records") == ["a-9", "m-1"]
            for i in items))

    def test_empty_snapshot_falls_back_to_entity_touch(self):
        self.srv.route("/data",
                       (200, ok_results([])),
                       (200, ok_results([{"xid": "au-1", "name": "N"}])))
        c = self.client()
        # author has no relations in SCHEMA -> no relation xids -> fallback
        qw = c.watch_query("author", {}, {"name": None})
        self.addCleanup(qw.close)
        qw.ready(timeout=5)
        self.assertEqual(qw.initial(), [])
        q = pump(qw.events())
        self.wait_set_post(lambda items: any(
            i.get("type") == "entity" and i.get("entities") == ["author"]
            for i in items))
        self.chan.send({"type": "entity/touched", "entity": "author",
                        "ts": "t1"})
        ev = q.get(timeout=5)
        self.assertEqual(ev["type"], "query/added")
        self.assertEqual(ev["record"], {"xid": "au-1", "name": "N"})

    def test_xsql_variant_and_required_entity(self):
        c = self.client()
        with self.assertRaises(SynthigyError) as cm:
            c.watch_query_xsql("movie\n  title\n")
        self.assertEqual(cm.exception.code, "INVALID_INTEREST")

        rows = [{"xid": "m-1", "title": "A"}]
        self.srv.route("/data", (200, ok_results(rows)))
        qw = c.watch_query_xsql("movie\n  title\n", {"p": 1}, entity="movie")
        self.addCleanup(qw.close)
        qw.ready(timeout=5)
        self.assertEqual(qw.initial(), rows)
        op = self.srv.requests_to("/data")[-1]
        body = json.loads(op["body"])["operations"][0]
        self.assertEqual(body["op"], "xsql")
        self.assertEqual(body["xsql"], "@search _q\nmovie\n  title\n")
        self.assertEqual(body["params"], {"p": 1})

    def test_bootstrap_error_surfaces_in_ready(self):
        self.srv.route("/data", (200, {"results": [{
            "ok": False, "error": {"message": "nope",
                                   "code": "UNKNOWN_ENTITY"}}]}))
        c = self.client()
        qw = c.watch_query("movie", {}, {"title": None})
        self.addCleanup(qw.close)
        with self.assertRaises(SynthigyError) as cm:
            qw.ready(timeout=5)
        self.assertEqual(cm.exception.code, "UNKNOWN_ENTITY")


class TestSqlTemplateWatch(WatchTestCase):
    def test_requires_entities_or_relations(self):
        c = self.client()
        with self.assertRaises(SynthigyError) as cm:
            c.watch_sql_template("SELECT COUNT(*) AS n FROM {movie}")
        self.assertEqual(cm.exception.code, "INVALID_INTEREST")

    def test_result_changed_on_touch(self):
        self.srv.route("/data",
                       (200, ok_results([{"n": 1}])),
                       (200, ok_results([{"n": 2}])))
        c = self.client()
        stw = c.watch_sql_template("SELECT COUNT(*) AS n FROM {movie}",
                                   entities=["movie"])
        self.addCleanup(stw.close)
        stw.ready(timeout=5)
        self.assertEqual(stw.value(), [{"n": 1}])
        self.assertEqual(stw.first(), {"n": 1})
        q = pump(stw.events())
        self.wait_set_post(lambda items: any(
            i.get("type") == "entity" and i.get("entities") == ["movie"]
            for i in items))
        self.chan.send({"type": "entity/touched", "entity": "movie",
                        "ts": "t9"})
        ev = q.get(timeout=5)
        self.assertEqual(ev["type"], "result/changed")
        self.assertEqual(ev["before"], [{"n": 1}])
        self.assertEqual(ev["after"], [{"n": 2}])
        self.assertEqual(ev["ts"], "t9")
        self.assertEqual(stw.value(), [{"n": 2}])
        self.assertEqual(stw.first(), {"n": 2})

    def test_identical_result_not_emitted(self):
        self.srv.route("/data", (200, ok_results([{"n": 1}])))
        c = self.client()
        stw = c.watch_sql_template("SELECT COUNT(*) AS n FROM {movie}",
                                   entities=["movie"])
        self.addCleanup(stw.close)
        stw.ready(timeout=5)
        q = pump(stw.events())
        self.wait_set_post(lambda items: any(
            i.get("type") == "entity" for i in items))
        self.chan.send({"type": "entity/touched", "entity": "movie",
                        "ts": "t1"})
        # refresh ran (2nd /data POST) but result is identical -> no event
        wait_until(lambda: len(self.srv.requests_to("/data")) >= 2)
        time.sleep(0.15)
        self.assertTrue(q.empty())

    def test_relations_track_interest(self):
        self.srv.route("/data", (200, ok_results([{"n": 1}])))
        c = self.client()
        stw = c.watch_sql_template("SELECT 1", relations=["Movie.actors"],
                                   records=["r-1"])
        self.addCleanup(stw.close)
        stw.ready(timeout=5)
        self.wait_set_post(lambda items: any(
            i.get("type") == "relation"
            and i.get("relations") == ["Movie.actors"] for i in items))
        self.wait_set_post(lambda items: any(
            i.get("type") == "data" and i.get("records") == ["r-1"]
            for i in items))


class TestModuleVerbs(WatchTestCase):
    def test_watch_verbs_delegate_to_default_client(self):
        synthigy.connect(self.srv.endpoint, token="tok")
        self.addCleanup(synthigy.disconnect)
        with self.assertRaises(SynthigyError) as cm:
            synthigy.watch_sql_template("SELECT 1")
        self.assertEqual(cm.exception.code, "INVALID_INTEREST")
        with self.assertRaises(SynthigyError) as cm:
            synthigy.watch({})
        self.assertEqual(cm.exception.code, "EMPTY_INTEREST")
        with self.assertRaises(SynthigyError) as cm:
            synthigy.watch_query_xsql("movie\n  title\n")
        self.assertEqual(cm.exception.code, "INVALID_INTEREST")


if __name__ == "__main__":
    unittest.main()
