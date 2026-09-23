"""client.login — authorization-code + PKCE against the real-socket stub."""

import asyncio
import hashlib
import json
import time
import unittest
import urllib.parse

import synthigy
from synthigy import AsyncClient, Client, MemoryLoginStore, SynthigyError
from synthigy.login import LoginStore, b64url

from test_client import StubServer

REDIRECT = "https://app.example/callback"


def id_token(claims):
    enc = lambda d: b64url(json.dumps(d).encode())
    return f"{enc({'alg': 'RS256'})}.{enc(claims)}.sig"


def query_of(url):
    return {k: v[0] for k, v in
            urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).items()}


class RecordingStore(MemoryLoginStore):
    def __init__(self):
        super().__init__()
        self.puts = []

    def put(self, state, login):
        self.puts.append((state, dict(login)))
        super().put(state, login)


class AsyncStore:
    def __init__(self):
        self.d = {}

    async def put(self, state, login):
        self.d[state] = login

    async def take(self, state):
        return self.d.pop(state, None)


class LoginTestCase(unittest.TestCase):
    def setUp(self):
        self.srv = StubServer()
        self.addCleanup(self.srv.close)
        self.store = RecordingStore()

    def client(self, **kw):
        kw.setdefault("client_id", "app")
        kw.setdefault("client_secret", "s3cret")
        kw.setdefault("login_store", self.store)
        c = Client(self.srv.endpoint, **kw)
        self.addCleanup(c.close)
        return c

    def route_token(self, nonce, **extra):
        claims = {"sub": "alice", "xid": "x-alice", "nonce": nonce}
        claims.update(extra.pop("claims", {}))
        body = {"access_token": "at", "token_type": "Bearer",
                "id_token": id_token(claims), "scope": "openid profile"}
        body.update(extra)
        self.srv.route("/oauth/token", (200, body))

    def started(self, c, **kw):
        url = c.login.start(redirect_uri=REDIRECT, **kw)["url"]
        q = query_of(url)
        return url, q, self.store.puts[-1][1]


class TestGuards(LoginTestCase):
    def test_requires_confidential_client(self):
        c = self.client(client_secret=None, token="tok")
        with self.assertRaises(SynthigyError) as cm:
            c.login.start(redirect_uri=REDIRECT)
        self.assertEqual(cm.exception.code, "LOGIN_REQUIRES_CONFIDENTIAL_CLIENT")

    def test_no_store_teaches(self):
        c = Client(self.srv.endpoint, client_id="app", client_secret="s")
        self.addCleanup(c.close)
        with self.assertRaises(SynthigyError) as cm:
            c.login.start(redirect_uri=REDIRECT)
        self.assertEqual(cm.exception.code, "NO_LOGIN_STORE")
        self.assertFalse(cm.exception.retryable)


class TestStart(LoginTestCase):
    def test_authorize_url_carries_all_params(self):
        url, q, _ = self.started(self.client(), scope="openid profile")
        self.assertTrue(url.startswith(self.srv.endpoint + "/oauth/authorize?"))
        self.assertEqual(q["response_type"], "code")
        self.assertEqual(q["client_id"], "app")
        self.assertEqual(q["redirect_uri"], REDIRECT)
        self.assertEqual(q["scope"], "openid profile")
        self.assertEqual(q["code_challenge_method"], "S256")
        for k in ("state", "code_challenge", "nonce"):
            self.assertTrue(q[k])
        self.assertEqual(self.srv.requests, [])

    def test_challenge_is_sha256_of_stored_verifier(self):
        _, q, pending = self.started(self.client())
        digest = hashlib.sha256(pending["code_verifier"].encode()).digest()
        self.assertEqual(q["code_challenge"], b64url(digest))
        self.assertEqual(q["nonce"], pending["nonce"])
        self.assertEqual(self.store.puts[-1][0], q["state"])

    def test_public_endpoint_for_browser_endpoint_for_exchange(self):
        c = self.client()
        url, q, pending = self.started(c, public_endpoint="https://id.example/")
        self.assertTrue(url.startswith("https://id.example/oauth/authorize?"))
        self.route_token(pending["nonce"])
        c.login.complete(code="c", state=q["state"], redirect_uri=REDIRECT)
        self.assertEqual(len(self.srv.requests_to("/oauth/token")), 1)


class TestComplete(LoginTestCase):
    def test_exchange_body_and_user(self):
        c = self.client()
        _, q, pending = self.started(c, return_to="/after")
        self.route_token(pending["nonce"])
        out = c.login.complete(code="the-code", state=q["state"],
                               redirect_uri=REDIRECT)
        self.assertEqual(out["user"], {"xid": "x-alice", "name": "alice",
                                       "scopes": ["openid", "profile"]})
        self.assertEqual(out["return_to"], "/after")
        self.assertEqual(out["tokens"]["access_token"], "at")
        req = self.srv.requests_to("/oauth/token")[-1]
        form = {k: v[0] for k, v in
                urllib.parse.parse_qs(req["body"].decode()).items()}
        self.assertEqual(form, {
            "grant_type": "authorization_code", "code": "the-code",
            "redirect_uri": REDIRECT, "client_id": "app",
            "client_secret": "s3cret",
            "code_verifier": pending["code_verifier"]})

    def test_exchange_sends_no_bearer(self):
        c = self.client(token="static-tok")
        _, q, pending = self.started(c)
        self.route_token(pending["nonce"])
        c.login.complete(code="c", state=q["state"], redirect_uri=REDIRECT)
        headers = {k.lower() for k in
                   self.srv.requests_to("/oauth/token")[-1]["headers"]}
        self.assertNotIn("authorization", headers)

    def test_nonce_mismatch(self):
        c = self.client()
        _, q, _ = self.started(c)
        self.route_token("someone-elses")
        with self.assertRaises(SynthigyError) as cm:
            c.login.complete(code="c", state=q["state"], redirect_uri=REDIRECT)
        self.assertEqual(cm.exception.code, "LOGIN_NONCE_MISMATCH")

    def test_unknown_state(self):
        with self.assertRaises(SynthigyError) as cm:
            self.client().login.complete(code="c", state="nope",
                                         redirect_uri=REDIRECT)
        self.assertEqual(cm.exception.code, "LOGIN_STATE_UNKNOWN")
        self.assertEqual(self.srv.requests_to("/oauth/token"), [])

    def test_state_is_one_shot(self):
        c = self.client()
        _, q, pending = self.started(c)
        self.route_token(pending["nonce"])
        c.login.complete(code="c", state=q["state"], redirect_uri=REDIRECT)
        with self.assertRaises(SynthigyError) as cm:
            c.login.complete(code="c", state=q["state"], redirect_uri=REDIRECT)
        self.assertEqual(cm.exception.code, "LOGIN_STATE_UNKNOWN")
        self.assertEqual(len(self.srv.requests_to("/oauth/token")), 1)

    def test_non_2xx_is_exchange_failed_without_retry(self):
        c = self.client()
        _, q, _ = self.started(c)
        self.srv.route("/oauth/token", (401, {"error": "invalid_grant"}))
        with self.assertRaises(SynthigyError) as cm:
            c.login.complete(code="c", state=q["state"], redirect_uri=REDIRECT)
        self.assertEqual(cm.exception.code, "LOGIN_EXCHANGE_FAILED")
        self.assertEqual(cm.exception.status, 401)
        self.assertEqual(len(self.srv.requests_to("/oauth/token")), 1)

    def test_missing_id_token(self):
        c = self.client()
        _, q, _ = self.started(c)
        self.srv.route("/oauth/token", (200, {"access_token": "at"}))
        with self.assertRaises(SynthigyError) as cm:
            c.login.complete(code="c", state=q["state"], redirect_uri=REDIRECT)
        self.assertEqual(cm.exception.code, "LOGIN_EXCHANGE_FAILED")

    def test_missing_xid_claim_refused(self):
        c = self.client()
        _, q, pending = self.started(c)
        self.route_token(pending["nonce"], claims={"xid": None})
        with self.assertRaises(SynthigyError) as cm:
            c.login.complete(code="c", state=q["state"], redirect_uri=REDIRECT)
        self.assertEqual(cm.exception.code, "LOGIN_EXCHANGE_FAILED")
        self.assertIn("xid", cm.exception.message)

    def test_scopes_fall_back_to_id_token(self):
        c = self.client()
        _, q, pending = self.started(c)
        self.route_token(pending["nonce"], scope=None,
                         claims={"scope": "openid email"})
        out = c.login.complete(code="c", state=q["state"], redirect_uri=REDIRECT)
        self.assertEqual(out["user"]["scopes"], ["openid", "email"])


class TestCancel(LoginTestCase):
    def test_cancel_returns_then_none(self):
        c = self.client()
        _, q, _ = self.started(c, return_to="/cart")
        self.assertEqual(c.login.cancel(q["state"]), {"return_to": "/cart"})
        self.assertIsNone(c.login.cancel(q["state"]))
        self.assertIsNone(c.login.cancel(None))


class TestStores(LoginTestCase):
    def test_memory_store_ttl(self):
        s = MemoryLoginStore(ttl_seconds=60)
        s.put("old", {"created_at": time.time() - 61})
        s.put("new", {"created_at": time.time()})
        self.assertIsNone(s.take("old"))
        self.assertIsNotNone(s.take("new"))
        self.assertIsNone(s.take("new"))

    def test_memory_store_sweeps_on_put(self):
        s = MemoryLoginStore(ttl_seconds=60)
        s.put("old", {"created_at": time.time() - 61})
        s.put("new", {"created_at": time.time()})
        self.assertNotIn("old", s._pending)

    def test_async_store_on_async_client(self):
        store = AsyncStore()
        self.assertIsInstance(store, LoginStore)

        async def run():
            async with AsyncClient(self.srv.endpoint, client_id="app",
                                   client_secret="s", login_store=store) as c:
                url = (await c.login.start(redirect_uri=REDIRECT))["url"]
                q = query_of(url)
                self.route_token(store.d[q["state"]]["nonce"])
                return await c.login.complete(code="c", state=q["state"],
                                              redirect_uri=REDIRECT)

        out = asyncio.run(run())
        self.assertEqual(out["user"]["xid"], "x-alice")
        self.assertEqual(store.d, {})


class TestLayers(LoginTestCase):
    def test_module_delegate(self):
        synthigy.connect(self.srv.endpoint, client_id="app",
                         client_secret="s", login_store=self.store)
        self.addCleanup(synthigy.disconnect)
        url = synthigy.login().start(redirect_uri=REDIRECT)["url"]
        self.assertIn("/oauth/authorize?", url)
        self.assertEqual(len(self.store.puts), 1)

    def test_facade_and_async_agree(self):
        facade_url = self.client().login.start(redirect_uri=REDIRECT)["url"]

        async def run():
            async with AsyncClient(self.srv.endpoint, client_id="app",
                                   client_secret="s3cret",
                                   login_store=self.store) as c:
                return (await c.login.start(redirect_uri=REDIRECT))["url"]

        async_url = asyncio.run(run())
        strip = lambda u: {k: v for k, v in query_of(u).items()
                           if k not in ("state", "nonce", "code_challenge")}
        self.assertEqual(strip(facade_url), strip(async_url))


if __name__ == "__main__":
    unittest.main()
