"""Client transport tests against a stdlib http.server stub — ports the
JS client.test.js matrix (envelopes, auth, error mapping, single-client).
`Client` is the blocking facade over the async engine, so this file IS the
"sync callers see no change" contract."""

import asyncio
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import synthigy
from synthigy import AsyncClient, Client, SynthigyError, ops


class _Stub(BaseHTTPRequestHandler):
    """Programmable stub. Class attrs are reset per test via StubServer."""
    server_version = "stub"

    def log_message(self, *a):  # silence
        pass

    def _read_body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n else b""

    def _respond(self, status, payload, headers=None):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        self._handle()

    def do_GET(self):
        self._handle()

    def _handle(self):
        srv = self.server
        record = {
            "path": self.path,
            "method": self.command,
            "headers": dict(self.headers),
            "body": self._read_body(),
        }
        srv.requests.append(record)
        path = self.path.split("?")[0]
        script = srv.routes.get(path)
        if script is None:
            self._respond(404, {"error": {"message": "no route",
                                          "code": "HTTP_ERROR"}})
            return
        if callable(script):
            script(self)  # custom handler (e.g. SSE streaming)
            return
        step = script[0] if len(script) == 1 else script.pop(0)
        status, payload, headers = (step + ({},))[:3] if len(step) == 2 else step
        self._respond(status, payload, headers)


class StubServer:
    def __init__(self):
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Stub)
        self.httpd.requests = []
        self.httpd.routes = {}
        self.thread = threading.Thread(
            target=lambda: self.httpd.serve_forever(poll_interval=0.05),
            daemon=True)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    @property
    def requests(self):
        return self.httpd.requests

    def route(self, path, *steps):
        """Each step is (status, payload) or (status, payload, headers).
        One step = repeat forever; several = consume in order. A single
        callable takes over the handler (SSE streaming)."""
        if len(steps) == 1 and callable(steps[0]):
            self.httpd.routes[path] = steps[0]
        else:
            self.httpd.routes[path] = list(steps)

    def requests_to(self, path):
        return [r for r in self.requests if r["path"].split("?")[0] == path]

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def ok_results(*data):
    return {"results": [{"ok": True, "data": d} for d in data]}


class ClientTestCase(unittest.TestCase):
    def setUp(self):
        self.srv = StubServer()
        self.addCleanup(self.srv.close)

    def static_client(self, **kw):
        return Client(self.srv.endpoint, token="tok", **kw)

    def last_data_body(self):
        return json.loads(self.srv.requests_to("/data")[-1]["body"])


class TestEnvelope(ClientTestCase):
    def test_search_envelope_and_default_empty(self):
        self.srv.route("/data", (200, ok_results(None)))
        c = self.static_client()
        out = c.search("User", {"active": {"_eq": True}}, {"name": None})
        self.assertEqual(out, [])
        body = self.last_data_body()
        self.assertEqual(body["operations"], [{
            "op": "search", "entity": "User",
            "args": {"active": {"_eq": True}},
            "selections": {"name": None}}])
        self.assertNotIn("acting_as", body)
        self.assertNotIn("key_format", body)

    def test_acting_as_only_when_set(self):
        self.srv.route("/data", (200, ok_results([])))
        c = self.static_client(acting_as="u-default")
        c.search("User", None, None)
        self.assertEqual(self.last_data_body()["acting_as"], "u-default")
        c.search("User", None, None, acting_as="u-override")
        self.assertEqual(self.last_data_body()["acting_as"], "u-override")

    def test_key_format_precedence(self):
        self.srv.route("/data", (200, ok_results([])))
        c = self.static_client(key_format="kebab")
        c.search("User", None, None)
        self.assertEqual(self.last_data_body()["key_format"], "kebab")
        c.search("User", None, None, key_format="camel")
        self.assertEqual(self.last_data_body()["key_format"], "camel")

    def test_get_flat_args_and_none(self):
        self.srv.route("/data", (200, ok_results(None)))
        c = self.static_client()
        out = c.get("User", {"xid": "u-1"}, {"name": None})
        self.assertIsNone(out)
        op = self.last_data_body()["operations"][0]
        self.assertEqual(op["args"], {"xid": "u-1"})  # flat, no _eq/_where

    def test_query_sends_xsql_document_op(self):
        # STRICT wire: XSQL travels only as {op: "xsql", xsql: <document>} —
        # a bare rooted body gets a synthetic @<op> _q header client-side.
        self.srv.route("/data", (200, ok_results([{"n": 1}])))
        c = self.static_client()
        c.query("user\n  name\n", {"a": True})
        op = self.last_data_body()["operations"][0]
        self.assertEqual(op["op"], "xsql")
        self.assertEqual(op["xsql"], "@search _q\nuser\n  name\n")
        self.assertEqual(op["params"], {"a": True})
        self.assertNotIn("selections", op)
        self.assertNotIn("entity", op)

    def test_query_document_source_passes_through(self):
        self.srv.route("/data", (200, ok_results([{"n": 1}])))
        c = self.static_client()
        c.query("@search list\nuser\n  name\n")
        op = self.last_data_body()["operations"][0]
        self.assertEqual(op["xsql"], "@search list\nuser\n  name\n")

    def test_query_get_mode_single(self):
        self.srv.route("/data", (200, ok_results(None)))
        c = self.static_client()
        self.assertIsNone(c.query("user\n  name\n", op="get"))
        op = self.last_data_body()["operations"][0]
        self.assertEqual(op["op"], "xsql")
        self.assertTrue(op["xsql"].startswith("@get _q\n"))

    def test_slice_selection_has_no_join_sugar(self):
        # the client injects no join for ANY op (flat-LEFT decree: join
        # semantics are the server's); slice pins it because the server
        # once rejected an injected _join here.
        self.srv.route("/data", (200, ok_results({"roles": True})))
        c = self.static_client()
        c.slice("User", {"xid": "u-1"}, {"roles": {"xid": None}})
        op = self.last_data_body()["operations"][0]
        self.assertEqual(op["op"], "slice")
        self.assertEqual(op["selections"],
                         {"roles": [{"selections": {"xid": None}}]})

    def test_sql_template_defaults_params(self):
        self.srv.route("/data", (200, ok_results([{"n": 2}])))
        c = self.static_client()
        out = c.sql_template("SELECT COUNT(*) AS n FROM {user}")
        self.assertEqual(out, [{"n": 2}])
        self.assertEqual(self.last_data_body()["operations"][0]["params"], [])

    def test_writes(self):
        self.srv.route("/data", (200, ok_results({"xid": "u-1"})))
        c = self.static_client()
        c.sync("User", {"xid": "u-1", "name": "A"})
        self.assertEqual(self.last_data_body()["operations"][0]["op"], "sync")
        c.stack("User", {"xid": "u-1"})
        self.assertEqual(self.last_data_body()["operations"][0]["op"], "stack")
        c.delete("User", {"xid": "u-1"})
        self.assertEqual(self.last_data_body()["operations"][0]["op"], "delete")

    def test_exec_batch_order(self):
        self.srv.route("/data", (200, ok_results({"xid": "a"}, True)))
        c = self.static_client()
        results = c.exec_([ops.stack("User", {"xid": "a"}),
                           ops.delete("User", {"xid": "b"})])
        self.assertTrue(results[0]["ok"])
        body = self.last_data_body()
        self.assertEqual([o["op"] for o in body["operations"]],
                         ["stack", "delete"])

    def test_request_id_header_sent(self):
        self.srv.route("/data", (200, ok_results([])))
        self.static_client().search("User", None, None)
        headers = self.srv.requests_to("/data")[-1]["headers"]
        self.assertTrue(headers.get("X-Request-Id"))


class TestErrors(ClientTestCase):
    def test_per_op_error(self):
        self.srv.route("/data", (200, {"results": [{
            "ok": False,
            "error": {"message": "nope", "code": "UNKNOWN_ENTITY",
                      "entity": "Uzer", "hint": "User"}}]}))
        with self.assertRaises(SynthigyError) as cm:
            self.static_client().search("Uzer", None, None)
        e = cm.exception
        self.assertEqual(e.code, "UNKNOWN_ENTITY")
        self.assertEqual(e.category, "not_found")
        self.assertEqual(e.hint, "User")
        self.assertFalse(e.retryable)

    def test_batch_continues_after_per_op_failure(self):
        self.srv.route("/data", (200, {"results": [
            {"ok": False, "error": {"message": "x", "code": "FK_VIOLATION"}},
            {"ok": True, "data": {"xid": "b"}}]}))
        results = self.static_client().exec_([ops.sync("User", {}),
                                              ops.sync("User", {"xid": "b"})])
        self.assertFalse(results[0]["ok"])
        self.assertTrue(results[1]["ok"])

    def test_403_forbidden(self):
        self.srv.route("/data", (403, {"error": {
            "message": "no scope", "code": "FORBIDDEN"}}))
        with self.assertRaises(SynthigyError) as cm:
            self.static_client().search("User", None, None)
        self.assertEqual(cm.exception.code, "FORBIDDEN")
        self.assertEqual(cm.exception.category, "iam")

    def test_403_unparseable_falls_back(self):
        self.srv.route("/data", (403, "not-a-dict"))
        with self.assertRaises(SynthigyError) as cm:
            self.static_client().search("User", None, None)
        self.assertEqual(cm.exception.code, "FORBIDDEN")

    def test_5xx_http_error_with_body_text(self):
        self.srv.route("/data", (500, "boom"))
        with self.assertRaises(SynthigyError) as cm:
            self.static_client().search("User", None, None)
        e = cm.exception
        self.assertEqual(e.code, "HTTP_ERROR")
        self.assertEqual(e.status, 500)
        self.assertTrue(e.retryable)

    def test_5xx_with_embedded_error(self):
        self.srv.route("/data", (500, {"error": {
            "message": "t", "code": "TIMEOUT"}}))
        with self.assertRaises(SynthigyError) as cm:
            self.static_client().search("User", None, None)
        self.assertEqual(cm.exception.code, "TIMEOUT")
        self.assertEqual(cm.exception.category, "rate_limit")

    def test_xsql_parse_error_position_fields(self):
        self.srv.route("/data", (200, {"results": [{
            "ok": False,
            "error": {"message": "bad", "code": "XSQL_PARSE_ERROR",
                      "line": 3, "col": 7, "diagnostics": [{"line": 3}]}}]}))
        with self.assertRaises(SynthigyError) as cm:
            self.static_client().query("nope")
        self.assertEqual((cm.exception.line, cm.exception.col), (3, 7))

    def test_network_error(self):
        c = Client("http://127.0.0.1:1", token="tok", timeout=0.2)
        with self.assertRaises(SynthigyError) as cm:
            c.search("User", None, None)
        self.assertEqual(cm.exception.code, "NETWORK_ERROR")
        self.assertTrue(cm.exception.retryable)


class TestAuth(ClientTestCase):
    def test_config_validation(self):
        # No token, no creds, no SYNTHIGY_TOKEN/SYNTHIGY_SUPERVISED in the
        # env — the teaching throw (PLAN-EXEC-IDENTITY.md step 3), not a
        # bare ValueError.
        with self.assertRaises(SynthigyError) as ctx:
            Client(self.srv.endpoint)
        self.assertEqual(ctx.exception.code, "NO_TOKEN")

    def test_static_empty_token_no_auth_header(self):
        self.srv.route("/data", (200, ok_results([])))
        Client(self.srv.endpoint, token="").search("User", None, None)
        headers = self.srv.requests_to("/data")[-1]["headers"]
        self.assertNotIn("Authorization", headers)

    def test_client_credentials_flow_and_cache(self):
        self.srv.route("/oauth/token",
                       (200, {"access_token": "t1", "expires_in": 3600}))
        self.srv.route("/data", (200, ok_results([])))
        c = Client(self.srv.endpoint, client_id="cid", client_secret="sec",
                   scope="data:read")
        c.search("User", None, None)
        c.search("User", None, None)
        token_reqs = self.srv.requests_to("/oauth/token")
        self.assertEqual(len(token_reqs), 1)  # cached on second call
        form = dict(p.split("=") for p in token_reqs[0]["body"].decode().split("&"))
        self.assertEqual(form["grant_type"], "client_credentials")
        self.assertEqual(form["client_id"], "cid")
        self.assertEqual(form["scope"], "data%3Aread")
        auth = self.srv.requests_to("/data")[-1]["headers"]["Authorization"]
        self.assertEqual(auth, "Bearer t1")

    def test_401_clears_cache_and_retries_once(self):
        self.srv.route("/oauth/token",
                       (200, {"access_token": "t1", "expires_in": 3600}),
                       (200, {"access_token": "t2", "expires_in": 3600}))
        self.srv.route("/data",
                       (401, {}),
                       (200, ok_results([{"xid": "u"}])))
        c = Client(self.srv.endpoint, client_id="cid", client_secret="sec")
        out = c.search("User", None, None)
        self.assertEqual(out, [{"xid": "u"}])
        self.assertEqual(len(self.srv.requests_to("/oauth/token")), 2)
        data_reqs = self.srv.requests_to("/data")
        self.assertEqual(len(data_reqs), 2)
        self.assertEqual(data_reqs[-1]["headers"]["Authorization"], "Bearer t2")

    def test_second_401_raises(self):
        self.srv.route("/oauth/token",
                       (200, {"access_token": "t", "expires_in": 3600}))
        self.srv.route("/data", (401, {}))
        c = Client(self.srv.endpoint, client_id="cid", client_secret="sec")
        with self.assertRaises(SynthigyError) as cm:
            c.search("User", None, None)
        self.assertEqual(cm.exception.code, "UNAUTHORIZED")

    def test_static_token_ignores_audience(self):
        self.assertEqual(self.static_client().token(audience="robotics"), "tok")

    def test_token_per_audience(self):
        self.srv.route("/oauth/token",
                       (200, {"access_token": "t-default", "expires_in": 3600}),
                       (200, {"access_token": "t-rob", "expires_in": 3600}))
        c = Client(self.srv.endpoint, client_id="cid", client_secret="sec")
        self.assertEqual(c.token(), "t-default")
        self.assertEqual(c.token(audience="robotics"), "t-rob")
        self.assertEqual(c.token(), "t-default")  # cached, no 3rd request
        body = self.srv.requests_to("/oauth/token")[1]["body"].decode()
        self.assertIn("audience=robotics", body)


class TestIntrospection(ClientTestCase):
    def test_schema_get_with_entities(self):
        self.srv.route("/schema", (200, {"id-key": "xid", "entities": {}}))
        out = self.static_client().schema(["user", "role"])
        self.assertEqual(out["id-key"], "xid")
        req = self.srv.requests_to("/schema")[-1]
        self.assertIn("entities=user%2Crole", req["path"])
        self.assertEqual(req["method"], "GET")

    def test_lint(self):
        self.srv.route("/lint", (200, {"diagnostics": [{"line": 1}]}))
        out = self.static_client().lint("user\n  nmae\n", entity="user")
        self.assertEqual(out, [{"line": 1}])
        body = json.loads(self.srv.requests_to("/lint")[-1]["body"])
        self.assertEqual(body["entity"], "user")


class TestOnboarding(ClientTestCase):
    def test_onboard_success(self):
        self.srv.route("/oauth/onboard",
                       (200, {"onboard_url": "http://x/oauth/claim?token=t",
                              "expires_at": 123, "user": {"xid": "user-xid-1"}}))
        out = self.static_client().onboard(
            "user-xid-1", methods=["password"], reset=True,
            ttl_seconds=600, return_url="https://app.example.com/callback")
        self.assertEqual(out["expires_at"], 123)
        self.assertEqual(out["user"]["xid"], "user-xid-1")
        req = self.srv.requests_to("/oauth/onboard")[-1]
        self.assertEqual(req["method"], "POST")
        body = json.loads(req["body"])
        self.assertEqual(body, {"xid": "user-xid-1",
                                "reset": True, "methods": ["password"],
                                "ttl_seconds": 600,
                                "return_url": "https://app.example.com/callback"})

    def test_onboard_omits_unset_optionals(self):
        self.srv.route("/oauth/onboard", (200, {"onboard_url": "x", "expires_at": 1}))
        self.static_client().onboard("user-xid-1")
        body = json.loads(self.srv.requests_to("/oauth/onboard")[-1]["body"])
        self.assertEqual(body, {"xid": "user-xid-1"})

    def test_onboard_forbidden_maps_to_synthigy_error(self):
        self.srv.route("/oauth/onboard", (403, {"error": "provision_forbidden"}))
        with self.assertRaises(SynthigyError) as ctx:
            self.static_client().onboard("user-xid-1")
        self.assertEqual(ctx.exception.code, "PROVISION_FORBIDDEN")
        self.assertEqual(ctx.exception.category, "auth")
        self.assertEqual(ctx.exception.status, 403)

    def test_onboard_unknown_xid_maps_to_synthigy_error(self):
        self.srv.route("/oauth/onboard", (404, {"error": "user_not_found"}))
        with self.assertRaises(SynthigyError) as ctx:
            self.static_client().onboard("does-not-exist")
        self.assertEqual(ctx.exception.code, "USER_NOT_FOUND")
        self.assertEqual(ctx.exception.category, "auth")
        self.assertEqual(ctx.exception.status, 404)

    def test_onboard_non_json_error_body_does_not_crash(self):
        # A route-not-found 404 (proxy/SPA fallback) returns plain text, not
        # the {"error": "..."} shape — must not raise a raw JSONDecodeError.
        def not_found(stub):
            body = b"Not found"
            stub.send_response(404)
            stub.send_header("Content-Type", "text/plain")
            stub.send_header("Content-Length", str(len(body)))
            stub.end_headers()
            stub.wfile.write(body)

        self.srv.route("/oauth/onboard", not_found)
        with self.assertRaises(SynthigyError) as ctx:
            self.static_client().onboard("user-xid-1")
        self.assertEqual(ctx.exception.code, "HTTP_ERROR")
        self.assertEqual(ctx.exception.status, 404)


class TestOnboardComplete(ClientTestCase):
    def test_onboard_complete_success(self):
        self.srv.route("/oauth/onboard/complete",
                       (200, {"user": {"xid": "user-xid-1"}, "active": True}))
        out = self.static_client().onboard_complete("the-ticket")
        self.assertEqual(out["active"], True)
        self.assertEqual(out["user"]["xid"], "user-xid-1")
        req = self.srv.requests_to("/oauth/onboard/complete")[-1]
        self.assertEqual(req["method"], "POST")
        body = json.loads(req["body"])
        self.assertEqual(body, {"ticket": "the-ticket"})

    def test_onboard_complete_wrong_client_maps_to_synthigy_error(self):
        self.srv.route("/oauth/onboard/complete", (400, {"error": "claim_invalid"}))
        with self.assertRaises(SynthigyError) as ctx:
            self.static_client().onboard_complete("not-my-ticket")
        self.assertEqual(ctx.exception.code, "CLAIM_INVALID")
        self.assertEqual(ctx.exception.category, "auth")
        self.assertEqual(ctx.exception.status, 400)


class TestTrees(ClientTestCase):
    def test_search_tree_composes_forest(self):
        flat = [{"xid": "r", "p": None}, {"xid": "k", "p": {"xid": "r"}}]
        self.srv.route("/data", (200, ok_results(flat)))
        forest = self.static_client().search_tree("Human", "p", {}, ["xid"])
        self.assertEqual(forest[0]["xid"], "r")
        self.assertEqual(forest[0]["_children"][0]["xid"], "k")
        op = self.last_data_body()["operations"][0]
        self.assertEqual((op["op"], op["on"]), ("search-tree", "p"))

    def test_get_tree_raw(self):
        flat = [{"xid": "r"}, {"xid": "k", "p": {"xid": "r"}}]
        self.srv.route("/data", (200, ok_results(flat)))
        out = self.static_client().get_tree("Human", "r", "p", ["xid"], raw=True)
        self.assertEqual(out, flat)
        op = self.last_data_body()["operations"][0]
        self.assertEqual((op["op"], op["root"]), ("get-tree", "r"))


class TestSingleClient(ClientTestCase):
    def tearDown(self):
        synthigy.disconnect()

    def test_not_connected(self):
        synthigy.disconnect()
        with self.assertRaises(SynthigyError) as cm:
            synthigy.search("User", None, None)
        self.assertEqual(cm.exception.code, "NOT_CONNECTED")

    def test_connect_and_module_verbs(self):
        self.srv.route("/data", (200, ok_results([{"xid": "u"}])))
        synthigy.connect(self.srv.endpoint, token="tok")
        self.assertEqual(synthigy.search("User", None, ["xid"]),
                         [{"xid": "u"}])
        self.assertIsNotNone(synthigy.get_client())

    def test_connect_destroys_previous(self):
        first = synthigy.connect(self.srv.endpoint, token="tok")
        closed = []
        first.close = lambda: closed.append(True)
        second = synthigy.connect(self.srv.endpoint, token="tok2")
        self.assertTrue(closed)
        self.assertIs(synthigy.get_client(), second)


class TestFacadeLifecycle(ClientTestCase):
    """The facade's one-loop-thread contract: lazy start (fork safety),
    idempotent close, restart-after-close, sync+async in one process."""

    @staticmethod
    def loop_threads():
        return [t for t in threading.enumerate()
                if t.name == "synthigy-loop" and t.is_alive()]

    def test_loop_thread_starts_lazily_and_stops_on_close(self):
        # Lazy start matters: uvicorn/gunicorn fork workers, and a thread
        # started in __init__ would be silently lost in the child.
        self.srv.route("/data", (200, ok_results([])))
        before = len(self.loop_threads())
        c = Client(self.srv.endpoint, token="tok")
        self.assertEqual(len(self.loop_threads()), before)  # no thread yet
        c.search("User", None, None)
        self.assertEqual(len(self.loop_threads()), before + 1)
        c.close()
        self.assertEqual(len(self.loop_threads()), before)

    def test_close_idempotent_and_restart(self):
        self.srv.route("/data", (200, ok_results([])))
        c = Client(self.srv.endpoint, token="tok")
        self.assertEqual(c.search("User", None, None), [])
        c.close()
        c.close()   # idempotent
        # the object stays usable — a later call transparently restarts
        self.assertEqual(c.search("User", None, None), [])
        c.close()

    def test_sync_and_async_interleaved_one_process(self):
        self.srv.route("/data", (200, ok_results([{"xid": "u"}])))
        c = self.static_client()
        self.addCleanup(c.close)
        self.assertEqual(c.search("User", None, ["xid"]), [{"xid": "u"}])

        async def arun():
            ac = AsyncClient(self.srv.endpoint, token="tok")
            try:
                return await ac.search("User", None, ["xid"])
            finally:
                await ac.close()

        self.assertEqual(asyncio.run(arun()), [{"xid": "u"}])
        self.assertEqual(c.search("User", None, ["xid"]), [{"xid": "u"}])


if __name__ == "__main__":
    unittest.main()
