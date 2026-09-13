"""OIDC authorization-code + PKCE flow for the datastar-music demo.

BFF pattern — the browser never sees a token:

  1. browser  GET  /login
  2. server   302  → Synthigy /oauth/authorize?response_type=code&…
  3. user logs in on Synthigy
  4. browser  GET  /auth/callback?code=…&state=…
  5. server   POST /oauth/token   (code, client_id, client_secret, verifier)
  6. server stores tokens in an in-memory session keyed by cookie
  7. server   302  → returnTo

Cookie is HttpOnly + SameSite=Lax; tokens stay server-side.

Sessions live in a dict mirrored to a JSON file next to this module, so
`uvicorn --reload` (which restarts the process on every code edit) does
NOT log everyone out. Production: swap for Redis/Postgres. Stdlib only
(urllib, hashlib, secrets, json, base64) — no auth library.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Sequence
from dataclasses import dataclass, field

SESSION_COOKIE = "datastar_music_sid"
SESSION_TTL = 8 * 60 * 60  # seconds


@dataclass
class User:
    name: str
    xid: str | None = None
    scopes: Sequence[str] = field(default_factory=list)


@dataclass
class Session:
    sid: str
    tokens: dict
    user: User
    expires_at: float


class AuthError(Exception):
    def __init__(self, msg: str, status: int = 400):
        super().__init__(msg)
        self.status = status


# sid → Session (file-mirrored) ; state → (verifier, return_to, exp) (memory —
# a login attempt in flight across a reload can just be retried)
_SESSIONS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              ".sessions.json")
_pending: dict[str, tuple[str, str, float]] = {}


def _load_sessions() -> dict[str, Session]:
    try:
        with open(_SESSIONS_FILE) as f:
            raw = json.load(f)
        return {sid: Session(sid, s["tokens"],
                             User(s["user"]["name"], s["user"].get("xid"),
                                  s["user"].get("scopes") or []),
                             s["expires_at"])
                for sid, s in raw.items()}
    except Exception:
        return {}


def _save_sessions() -> None:
    data = {sid: {"tokens": s.tokens, "expires_at": s.expires_at,
                  "user": {"name": s.user.name, "xid": s.user.xid,
                           "scopes": list(s.user.scopes)}}
            for sid, s in _sessions.items()}
    try:
        tmp = _SESSIONS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, _SESSIONS_FILE)
    except OSError:
        pass


_sessions: dict[str, Session] = _load_sessions()


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _sweep() -> None:
    now = time.time()
    expired = [k for k, s in _sessions.items() if s.expires_at < now]
    for sid in expired:
        _sessions.pop(sid, None)
    if expired:
        _save_sessions()
    for st in [k for k, (_, _, exp) in _pending.items() if exp < now]:
        _pending.pop(st, None)


def get_session(sid: str | None) -> Session | None:
    if not sid:
        return None
    sess = _sessions.get(sid)
    if not sess:
        return None
    if sess.expires_at < time.time():
        _sessions.pop(sid, None)
        return None
    return sess


def drop_session(sid: str | None) -> None:
    if sid and _sessions.pop(sid, None):
        _save_sessions()


def start_login(endpoint: str, client_id: str, redirect_uri: str,
                return_to: str = "/", scope: str = "openid email profile") -> str:
    """Returns the Synthigy authorize URL to 302 the browser to."""
    _sweep()
    state = _b64url(secrets.token_bytes(24))
    verifier = _b64url(secrets.token_bytes(48))
    challenge = _b64url(hashlib.sha256(verifier.encode()).digest())
    _pending[state] = (verifier, return_to, time.time() + 300)
    q = urllib.parse.urlencode({
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scope,
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    })
    return f"{endpoint}/oauth/authorize?{q}"


async def complete_login(endpoint: str, client_id: str, client_secret: str,
                         redirect_uri: str, code: str, state: str,
                         resolve_user=None) -> tuple[str, str]:
    """Exchange code→tokens, resolve the user's xid, store a session.
    Returns (sid, return_to). `resolve_user(name)` is an ASYNC callable
    mapping the id_token sub (a username) to a record with an xid — the
    BFF needs it for acting_as.
    """
    pending = _pending.pop(state, None)
    if not pending:
        raise AuthError("Login state expired or unknown", 400)
    verifier, return_to, _ = pending

    body = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "client_secret": client_secret,
        "code_verifier": verifier,
    }).encode()
    req = urllib.request.Request(
        f"{endpoint}/oauth/token", data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"})

    def exchange():
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            raise AuthError(
                f"Token exchange failed ({e.code}): {e.read().decode()}", 500)

    # urllib is blocking — hop off the loop for this one-shot, rare POST
    # (auth plumbing stays stdlib; the SDK data-path is native async).
    tokens = await asyncio.to_thread(exchange)

    # Synthigy's id_token `sub` is the username, not the xid. Resolve the
    # xid via /data so we can act on the user's behalf.
    username = _id_token_sub(tokens.get("id_token"))
    user = User(name=username or "?",
                scopes=(tokens.get("scope") or "").split())
    if resolve_user and username:
        try:
            resolved = await resolve_user(username)
            if resolved and resolved.get("xid"):
                user.xid = resolved["xid"]
                user.name = resolved.get("name") or username
        except Exception:
            pass  # fall back to name-only; acting_as omitted

    sid = _b64url(secrets.token_bytes(24))
    _sessions[sid] = Session(sid, tokens, user, time.time() + SESSION_TTL)
    _save_sessions()
    return sid, return_to


def cookie_header(sid: str) -> str:
    return (f"{SESSION_COOKIE}={sid}; Path=/; HttpOnly; SameSite=Lax; "
            f"Max-Age={SESSION_TTL}")


def clear_cookie_header() -> str:
    return f"{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"


def _id_token_sub(id_token: str | None) -> str | None:
    if not id_token or not isinstance(id_token, str):
        return None
    parts = id_token.split(".")
    if len(parts) < 2:
        return None
    try:
        pad = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(pad))
        return payload.get("sub")
    except Exception:
        return None
