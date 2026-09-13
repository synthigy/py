"""Live integration suite — runs against a real Synthigy server, on BOTH
surfaces: the blocking facade Client and the native AsyncClient (one
engine underneath).

Skipped unless credentials are provided (mirrors sdk/go -tags integration
and sdk/js test:live). Never share identity with a live app — register a
dedicated OAuth client (trusted confidential, client_credentials).

    SYNTHIGY_TEST_ENDPOINT       (default http://localhost:7887)
    SYNTHIGY_TEST_CLIENT_ID      \\ client-credentials pair; both required
    SYNTHIGY_TEST_CLIENT_SECRET  /  or the suite self-skips

Run: python3 -m unittest tests.test_integration -v
"""

import asyncio
import os
import unittest

from synthigy import AsyncClient, Client, SynthigyError, eq, new_xid

ENDPOINT = os.environ.get("SYNTHIGY_TEST_ENDPOINT", "http://localhost:7887")
CLIENT_ID = os.environ.get("SYNTHIGY_TEST_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SYNTHIGY_TEST_CLIENT_SECRET")

# onboard() is gated on the client's principal administering the account —
# RBAC update on User plus the row inside its owner-group write scope, the
# shape the shipped User Provisioner role grants. The CLIENT_ID/SECRET
# identity above is ROOT, so onboarding runs on its OWN dedicated pair to
# exercise the real, scoped principal. Creating it (one-time, via nREPL):
#
#   (access/with-principal nil
#     (let [id "synthigy-py-sdk-provisioner"
#           role (dataset/get-entity :iam/user-role {:name "User Provisioner"} {(id/key) nil})
#           owners (dataset/sync-entity :iam/user-group {:name (str id "-owners")})
#           client (iam/add-client {:id id :name "Synthigy Python SDK provisioner"
#                                   :type :confidential
#                                   :settings {"allowed-grants" ["client_credentials"]}})]
#       (dataset/stack-entity :iam/user {:name id
#                                        :roles [{(id/key) (id/extract role)}]
#                                        :groups [{(id/key) (id/extract owners)}]})
#       (access/load-rules)
#       client))
PROVISION_CLIENT_ID = os.environ.get("SYNTHIGY_TEST_PROVISION_CLIENT_ID")
PROVISION_CLIENT_SECRET = os.environ.get("SYNTHIGY_TEST_PROVISION_CLIENT_SECRET")


@unittest.skipUnless(CLIENT_ID and CLIENT_SECRET,
                     "set SYNTHIGY_TEST_CLIENT_ID/SECRET to run live tests")
class TestLive(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client(ENDPOINT, client_id=CLIENT_ID,
                            client_secret=CLIENT_SECRET, timeout=15)

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "client"):
            cls.client.close()

    def _any_entity(self):
        schema = self.client.schema()
        names = sorted(schema["entities"].keys())
        self.assertTrue(names, "server has no readable entities")
        return names[0]

    def test_token_mint(self):
        tok = self.client.token()
        self.assertTrue(tok and isinstance(tok, str))

    def test_schema_shape(self):
        schema = self.client.schema()
        self.assertIn("entities", schema)
        self.assertIn("id-key", schema)

    def test_search_projects_id_key(self):
        rows = self.client.search(self._any_entity(), {"_limit": 1}, None)
        self.assertIsInstance(rows, list)
        if rows:  # xid is structural — always projected
            self.assertTrue("xid" in rows[0] or "euuid" in rows[0])

    def test_sql_template_count(self):
        rows = self.client.sql_template(
            "SELECT COUNT(*) AS n FROM {%s}" % self._any_entity())
        self.assertEqual(len(rows), 1)
        self.assertIn("n", rows[0])

    def test_aggregate_op_gone(self):
        # the standalone aggregate op is dead (UNKNOWN_OP) and the SDK
        # deliberately has no aggregate()/count() methods
        self.assertFalse(hasattr(self.client, "aggregate"))
        self.assertFalse(hasattr(self.client, "count"))
        result = self.client.exec_([{"op": "aggregate", "entity": "user",
                                     "args": {}, "selections": {}}])[0]
        self.assertFalse(result.get("ok"))
        self.assertEqual(result["error"]["code"], "UNKNOWN_OP")

    def test_write_cycle(self):
        # This suite was read-only for its whole life, which is exactly why the
        # 2026-08-29 silent-write flip went unnoticed here for two weeks while
        # the JS suite caught it on day one: a read-only live suite cannot see
        # write-contract drift. Pins BOTH halves of the returning contract.
        import time
        xid = new_xid()
        name = f"__py_sdk_wc_{time.time_ns()}__"
        self.addCleanup(lambda: self.client.purge("user", {"xid": eq(xid)}))

        silent = self.client.sync(
            "user", {"xid": xid, "name": name, "type": "ROBOT", "active": True})
        self.assertEqual(silent, {"count": 1},
                         "flagless sync must answer {'count': n}, not the record")

        got = self.client.get("user", {"xid": xid},
                              {"xid": None, "name": None, "type": None, "active": None})
        self.assertEqual(got["name"], name)
        self.assertEqual(got["type"], "ROBOT")
        self.assertTrue(got["active"])

        self.client.stack("user", {"xid": xid, "active": False})
        self.assertFalse(self.client.get("user", {"xid": xid}, {"active": None})["active"])

        echoed = self.client.sync(
            "user", {"xid": xid, "name": name + "-edited", "type": "ROBOT",
                     "active": True},
            returning=True)
        self.assertEqual(echoed["xid"], xid)
        self.assertEqual(echoed["name"], name + "-edited")

        self.client.delete("user", {"xid": xid})

    def test_acting_as_bogus_user(self):
        with self.assertRaises(SynthigyError) as cm:
            self.client.search("user", {"_limit": 1}, None,
                               acting_as="no-such-user-xid")
        self.assertIn(cm.exception.code,
                      ("USER_NOT_FOUND", "NOT_TRUSTED",
                       "PUBLIC_CLIENT_FORBIDDEN"))

    def test_left_join_sugar_preserves_parents(self):
        # a projected relation must not INNER-drop parents: plain count ==
        # left count, and explicit inner <= plain
        schema = self.client.schema()
        entity = rel_name = None
        for name in sorted(schema["entities"]):
            rels = schema["entities"][name].get("relations") or {}
            if rels:
                entity, rel_name = name, sorted(rels)[0]
                break
        if not entity:
            self.skipTest("no entity with relations readable by this client")
        plain = self.client.search(entity, {"_limit": 50}, None)
        left = self.client.search(entity, {"_limit": 50},
                                  {rel_name: {"xid": None}})
        self.assertEqual(len(plain), len(left))

    def test_listen_emits_open(self):
        for ev in self.client.listen():
            self.assertEqual(ev, {"type": "sse/open"})
            break

    def test_watch_sql_template_live_transport(self):
        entity = self._any_entity()
        w = self.client.watch_sql_template(
            "SELECT COUNT(*) AS n FROM {%s}" % entity, entities=[entity])
        try:
            w.ready(timeout=15)
            val = w.value()
            self.assertEqual(len(val), 1)
            self.assertIn("n", val[0])
        finally:
            w.close()


@unittest.skipUnless(CLIENT_ID and CLIENT_SECRET,
                     "set SYNTHIGY_TEST_CLIENT_ID/SECRET to run live tests")
class TestLiveAsync(unittest.TestCase):
    """Same server, native async surface — proves the engine live (real
    chunked SSE, real OAuth) without the facade in between."""

    def test_async_surface_end_to_end(self):
        async def run():
            # async with — closes the pool/mux on scope exit, like httpx.
            async with AsyncClient(ENDPOINT, client_id=CLIENT_ID,
                                   client_secret=CLIENT_SECRET,
                                   timeout=15) as client:
                schema = await client.schema()
                self.assertIn("entities", schema)
                entity = sorted(schema["entities"].keys())[0]
                rows = await client.search(entity, {"_limit": 1}, None)
                self.assertIsInstance(rows, list)
                counted = await client.sql_template(
                    "SELECT COUNT(*) AS n FROM {%s}" % entity)
                self.assertIn("n", counted[0])
                # live watch bootstrap over the real (chunked) SSE
                async with client.watch_query(entity, {"_limit": 1}) as w:
                    self.assertIsInstance(w.list(), list)

        asyncio.run(run())


@unittest.skipUnless(PROVISION_CLIENT_ID and PROVISION_CLIENT_SECRET,
                     "set SYNTHIGY_TEST_PROVISION_CLIENT_ID/SECRET "
                     "(a confidential client holding User Provisioner) to run")
class TestLiveOnboarding(unittest.TestCase):
    """Proves onboard() against a real scoped provisioner. Onboarding no
    longer creates accounts (PLAN-ONBOARDING.md P1) — each test creates its
    own account via sync() first, stamped with the client's own owner group
    so it lands inside the principal's write scope, then mints a ticket for
    its xid."""

    @classmethod
    def setUpClass(cls):
        cls.client = Client(ENDPOINT, client_id=PROVISION_CLIENT_ID,
                            client_secret=PROVISION_CLIENT_SECRET, timeout=15)
        # The client's own first group; None for an unscoped (superuser) client.
        me = cls.client.get("user", {"name": PROVISION_CLIENT_ID},
                            {"groups": {"xid": None}})
        groups = (me or {}).get("groups") or []
        cls.owner_group = groups[0]["xid"] if groups else None
        cls._root = (Client(ENDPOINT, client_id=CLIENT_ID,
                            client_secret=CLIENT_SECRET, timeout=15)
                     if CLIENT_ID and CLIENT_SECRET else None)

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "client"):
            cls.client.close()
        if getattr(cls, "_root", None):
            cls._root.close()

    def create_account(self, username):
        # Writes are silent by default, so mint the id up front rather than
        # reading it back out of an echo that isn't there.
        xid = new_xid()
        account = {"xid": xid, "name": username, "active": False}
        if self.owner_group:
            account["owner_group"] = {"xid": self.owner_group}
        self.client.sync("user", account)
        # Cleanup runs as ROOT, not as the provisioner: User Provisioner grants
        # create/read/update on User but deliberately NOT delete, so a
        # provisioner-run purge fails with insufficient privileges. These tests
        # ran as skips for their whole life, so nothing ever noticed them
        # piling up accounts on the dev tenant.
        if self._root:
            self.addCleanup(lambda: self._root.purge("user", {"xid": eq(xid)}))
        return {"xid": xid}

    def test_onboard_mint_and_unknown_xid(self):
        import time
        username = f"sdk-onboard-test-{int(time.time())}@example.com"
        created = self.create_account(username)

        out = self.client.onboard(created["xid"], methods=["password"])
        self.assertIn("onboard_url", out)
        self.assertIn("/oauth/claim?token=", out["onboard_url"])
        self.assertIsInstance(out["expires_at"], int)
        self.assertEqual(out["user"]["xid"], created["xid"])

        with self.assertRaises(SynthigyError) as ctx:
            self.client.onboard("does-not-exist-xid")
        self.assertEqual(ctx.exception.code, "USER_NOT_FOUND")

    def test_onboard_complete_redeems_own_ticket(self):
        import time
        import urllib.parse

        username = f"sdk-onboard-complete-test-{int(time.time())}@example.com"
        created = self.create_account(username)
        out = self.client.onboard(created["xid"], methods=["password"])
        query = urllib.parse.urlparse(out["onboard_url"]).query
        ticket = urllib.parse.parse_qs(query)["token"][0]

        result = self.client.onboard_complete(ticket)
        self.assertTrue(result["active"])
        self.assertEqual(result["user"]["xid"], created["xid"])


if __name__ == "__main__":
    unittest.main()
