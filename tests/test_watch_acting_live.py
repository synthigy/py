"""Live: a trusted client's live XSQL list acting as a user sees that user's
rows only (RLS), on the snapshot and on every refetch, with the options
generated watch_* ops pass (entity=, acting_as=).

Needs the guarded Project Management model (examples/datasets/
project-management@0.1.2 + seed/) and a trusted superuser client:
    SYNTHIGY_RLS_TEST_ENDPOINT
    SYNTHIGY_RLS_TEST_CLIENT_ID / SYNTHIGY_RLS_TEST_CLIENT_SECRET
    SYNTHIGY_RLS_TEST_ALICE / SYNTHIGY_RLS_TEST_BOB   (user xids)
Run: python -m unittest tests.test_watch_acting_live -v
"""

import os
import queue
import threading
import time
import unittest

from synthigy import Client, new_xid

ENDPOINT = os.environ.get("SYNTHIGY_RLS_TEST_ENDPOINT")
ALICE = os.environ.get("SYNTHIGY_RLS_TEST_ALICE")
BOB = os.environ.get("SYNTHIGY_RLS_TEST_BOB")
LIST = "@search list\nproject\n  xid\n  name\n  description"


def names(rows):
    return sorted(r["name"] for r in rows)


def find(rows, name):
    return next((r for r in rows if r["name"] == name), None)


@unittest.skipUnless(ENDPOINT, "set SYNTHIGY_RLS_TEST_ENDPOINT, the client and the ALICE/BOB xids")
class TestWatchActingLive(unittest.TestCase):
    def test_snapshot_and_refetch_run_as_the_user(self):
        c = Client(ENDPOINT, client_id=os.environ["SYNTHIGY_RLS_TEST_CLIENT_ID"],
                   client_secret=os.environ["SYNTHIGY_RLS_TEST_CLIENT_SECRET"], timeout=30)
        self.addCleanup(c.close)
        plain = c.query(LIST, acting_as=ALICE)
        alpha = find(plain, "Project Alpha")
        assert alpha is not None
        self.assertIsNone(find(plain, "Project Beta"), "RLS hides bob's project from alice")
        beta = find(c.query(LIST, acting_as=BOB), "Project Beta")
        assert beta is not None, "bob sees his own project"

        w = c.watch_query_xsql(LIST, {}, entity="project", acting_as=ALICE)
        self.addCleanup(w.close)
        w.ready(timeout=10)
        self.assertEqual(names(w.list()), names(plain), "the watch snapshot is the same rows as the plain query")

        run = "py-" + new_xid()
        c.sync("project", {"xid": alpha["xid"], "description": f"alice {run}"}, acting_as=ALICE)
        c.sync("project", {"xid": beta["xid"], "description": f"bob {run}"}, acting_as=BOB)
        q = queue.Queue()
        threading.Thread(target=lambda: [q.put(ev) for ev in w.events()], daemon=True).start()
        deadline = time.monotonic() + 10
        while (w.records.get(alpha["xid"]) or {}).get("description") != f"alice {run}":
            left = deadline - time.monotonic()
            self.assertGreater(left, 0, f"alice's change never reached her watch: {w.list()}")
            try:
                q.get(timeout=left)
            except queue.Empty:
                pass
        self.assertEqual(names(w.list()), names(plain), "the refetch is still exactly alice's rows")
        self.assertNotIn(beta["xid"], w.records, "bob's project never reaches alice's watch")
