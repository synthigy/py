"""OIDC authorization-code + PKCE — the protocol half of a browser login.

What this owns: the PKCE pair, the authorize URL, the code exchange, and
reading the subject out of the id_token. What it deliberately does NOT own:
sessions, cookies, redirects, routing. Those are your app's, and they are
where a framework dependency would come from. The one piece of state the
protocol itself needs — the in-flight login, from /login until the callback
comes back — you supply as a `login_store`.

Stdlib only, like the rest of the package.
"""

import base64
import hashlib
import inspect
import json
import secrets
import time
import urllib.parse
from typing import Any, Protocol, runtime_checkable

from .errors import SynthigyError

PKCE_VERIFIER_BYTES = 32
STATE_BYTES = 24
# must exceed the server's 8-minute login window, or slow logins fail LOGIN_STATE_UNKNOWN
LOGIN_TTL_SECONDS = 600


def b64url(raw):
    """Base64url-encode bytes without padding."""
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def b64url_decode(text):
    """Decode unpadded base64url text back to bytes."""
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def random_token(n):
    """A base64url token over `n` cryptographically random bytes."""
    return b64url(secrets.token_bytes(n))


def pkce():
    """A fresh PKCE verifier and its S256 challenge (RFC 7636)."""
    verifier = random_token(PKCE_VERIFIER_BYTES)
    challenge = b64url(hashlib.sha256(verifier.encode("ascii")).digest())
    return verifier, challenge


def decode_jwt_payload(jwt):
    """Read a JWT's payload without verifying its signature.

    Safe here and nowhere else: this token arrived in the body of our own
    client-authenticated POST to the token endpoint, over TLS, so its origin
    is already established by the channel. A signature check would be
    guarding against an attacker who can already answer as the IdP. Never use
    this on a token that reached you any other way — verifying those stays
    out of the SDK's scope, and PyJWT is the library for it.
    """
    parts = str(jwt or "").split(".")
    if len(parts) < 2:
        return None
    try:
        return json.loads(b64url_decode(parts[1]))
    except (ValueError, TypeError):
        return None


async def maybe_await(value):
    """Await `value` when it is awaitable, else pass it through."""
    return await value if inspect.isawaitable(value) else value


@runtime_checkable
class LoginStore(Protocol):
    """Where in-flight logins live between `start()` and the callback.

    A structural type on purpose: both methods may be sync OR async, so a
    dict-backed store and a `redis.asyncio` one satisfy it equally. `take`
    must be one-shot — return the login AND remove it, so a replayed
    callback finds nothing.

    The login is a plain dict (`code_verifier`, `nonce`, `return_to`,
    `created_at` epoch seconds) because your store round-trips it through
    Redis or JSON.
    """

    def put(self, state: str, login: dict) -> Any: ...

    def take(self, state: str) -> Any: ...


class MemoryLoginStore:
    """In-flight logins in a dict — SINGLE PROCESS ONLY.

    Every worker behind a load balancer gets its own dict, so a callback that
    lands on a different one than served /login fails with
    LOGIN_STATE_UNKNOWN. Real deployments back the store with whatever
    already holds their sessions (Redis, Postgres, a signed cookie).
    """

    def __init__(self, ttl_seconds=LOGIN_TTL_SECONDS):
        self._ttl = ttl_seconds
        self._pending = {}

    def put(self, state, login):
        # Swept on write rather than on a timer: a timer would need an event
        # loop and someone to tear it down.
        cutoff = time.time() - self._ttl
        for key in [k for k, v in self._pending.items()
                    if v.get("created_at", 0) <= cutoff]:
            del self._pending[key]
        self._pending[state] = login

    def take(self, state):
        login = self._pending.pop(state, None)
        if login is None:
            return None
        return None if login.get("created_at", 0) <= time.time() - self._ttl else login


def no_login_store_error():
    """The teaching throw for a client with no login store configured."""
    return SynthigyError(
        "no login store: pass login_store= to Client/AsyncClient — an object "
        "with put(state, login) and take(state), backed by whatever already "
        "holds your sessions. For a single-process dev server use "
        "synthigy.MemoryLoginStore().",
        "NO_LOGIN_STORE")


class RejectingLoginStore:
    """The default — refuses rather than silently keeping logins in one process."""

    def put(self, state, login):
        raise no_login_store_error()

    def take(self, state):
        raise no_login_store_error()


REJECTING_LOGIN_STORE = RejectingLoginStore()


class AsyncLoginAPI:
    """`client.login` — start / complete / cancel."""

    def __init__(self, client):
        self._client = client

    def _require_confidential(self):
        c = self._client
        if not (c._client_id and c._client_secret):
            raise SynthigyError(
                "login needs client_id + client_secret: the code exchange "
                "authenticates this process as a confidential client. A "
                "browser or native app is a public client and drives the same "
                "flow with a PKCE library of its own.",
                "LOGIN_REQUIRES_CONFIDENTIAL_CLIENT")

    async def start(self, *, redirect_uri, return_to="/", scope="openid",
                    public_endpoint=None):
        """Begin a login; redirect the browser to the returned URL."""
        self._require_confidential()
        if not redirect_uri:
            raise SynthigyError(
                "login.start needs redirect_uri — the URL on YOUR server the "
                "IdP sends the browser back to, registered on this OAuth "
                "client.",
                "LOGIN_REQUIRES_CONFIDENTIAL_CLIENT")
        c = self._client
        state = random_token(STATE_BYTES)
        nonce = random_token(STATE_BYTES)
        verifier, challenge = pkce()
        # Stored BEFORE the URL is handed out: a browser that follows the
        # redirect faster than the store commits would come back to nothing.
        await maybe_await(c._login_store.put(state, {
            "code_verifier": verifier,
            "nonce": nonce,
            "return_to": return_to,
            "created_at": int(time.time()),
        }))
        query = urllib.parse.urlencode([
            ("response_type", "code"),
            ("client_id", c._client_id),
            ("redirect_uri", redirect_uri),
            ("scope", scope),
            ("state", state),
            ("code_challenge", challenge),
            ("code_challenge_method", "S256"),
            ("nonce", nonce),
        ])
        base = (public_endpoint or c._endpoint).rstrip("/")
        return {"url": base + "/oauth/authorize?" + query}

    async def complete(self, *, code, state, redirect_uri):
        """Exchange the callback's code for tokens and the user behind them.

        `user["xid"]` comes straight out of the id_token — the engine puts it
        on every token it signs — so this costs no /data round-trip.
        """
        self._require_confidential()
        c = self._client
        pending = await self._take(state)
        if not pending:
            raise SynthigyError(
                "unknown or expired login state: the store never saw it, it "
                "was already consumed, or it timed out. A callback landing on "
                "a different worker than the one that served /login does this.",
                "LOGIN_STATE_UNKNOWN")

        form = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": c._client_id,
            "client_secret": c._client_secret,
            "code_verifier": pending["code_verifier"],
        }
        started = time.monotonic()
        # The transport seam, not _authed: that one attaches a bearer, mints
        # one first, and on 401 retries — which would replay a one-shot
        # authorization code the IdP has already burned.
        status, _h, data = await c._pool.request(
            "POST", c._endpoint + "/oauth/token",
            body=urllib.parse.urlencode(form).encode(),
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        if c._on_op:
            try:
                c._on_op({"path": "/oauth/token", "status": status,
                          "duration_ms": int((time.monotonic() - started) * 1000)})
            except Exception:
                pass

        if not 200 <= status < 300:
            raise SynthigyError(
                f"authorization-code exchange failed ({status})",
                "LOGIN_EXCHANGE_FAILED", data.decode("utf-8", "replace"),
                status=status)
        tokens = json.loads(data)
        claims = decode_jwt_payload(tokens.get("id_token"))
        if claims is None:
            raise SynthigyError(
                "token response carried no readable id_token — request the "
                "openid scope, which is what names the user.",
                "LOGIN_EXCHANGE_FAILED")
        if not claims.get("xid"):
            raise SynthigyError(
                "id_token carries no xid claim — without it the user can't be "
                "named in acting_as, and calls would silently run as this "
                "service.",
                "LOGIN_EXCHANGE_FAILED")
        if claims.get("nonce") != pending["nonce"]:
            raise SynthigyError(
                "id_token nonce does not match the one this login started with",
                "LOGIN_NONCE_MISMATCH")

        # Granted scopes live in the token response; the id_token carries
        # them only when the IdP chose to mirror them.
        raw_scope = tokens.get("scope") or claims.get("scope") or ""
        return {
            "user": {
                "xid": claims.get("xid"),
                "name": claims.get("sub"),
                "scopes": [s for s in raw_scope.split(" ") if s],
            },
            "tokens": tokens,
            "return_to": pending["return_to"],
        }

    async def cancel(self, state):
        """Discard an in-flight login and recover where the visitor was headed.

        Cancelling on the IdP's login page is a normal outcome (RFC 6749
        4.1.2.1 sends `error` back with no code), so an unknown state is None
        here rather than a raise.
        """
        pending = await self._take(state)
        return {"return_to": pending["return_to"]} if pending else None

    async def _take(self, state):
        """Take a pending login, refusing one older than LOGIN_TTL_SECONDS even when the store kept it."""
        pending = await maybe_await(self._client._login_store.take(state)) if state else None
        if pending and pending.get("created_at", 0) > time.time() - LOGIN_TTL_SECONDS:
            return pending
        return None
