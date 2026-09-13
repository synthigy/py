"""Token manager — OAuth client-credentials against /oauth/token, and the
supervised-stdio source for a child running under `synthigy exec`/`agent`
(or the EYWA robotics commander). See docs/plans/PLAN-EXEC-IDENTITY.md
steps 2-4 for the full contract.

Per-audience cache with a 30s pre-expiry buffer. The lock is held across
the whole refresh (single-flight) so concurrent callers can't stampede
the token endpoint — the race the JS SDK originally had.
"""

import json
import queue
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from .errors import SynthigyError


class TokenManager:
    def __init__(self, token_url, client_id, client_secret, scope=None,
                 timeout=None):
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._timeout = timeout
        self._tokens = {}  # audience -> (token, expires_at)
        self._lock = threading.Lock()

    def get_token(self, audience=None):
        key = audience or ""
        with self._lock:
            cached = self._tokens.get(key)
            if cached and time.time() < cached[1] - 30:
                return cached[0]

            params = {
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            }
            if self._scope:
                params["scope"] = self._scope
            if audience:
                params["audience"] = audience

            req = urllib.request.Request(
                self._token_url,
                data=urllib.parse.urlencode(params).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    data = json.loads(resp.read())
            except urllib.error.HTTPError as e:
                text = e.read().decode(errors="replace")
                raise RuntimeError(
                    f"Token request failed ({e.code}): {text}") from None

            token = data["access_token"]
            self._tokens[key] = (token, time.time() + data.get("expires_in", 3600))
            return token

    def clear(self):
        with self._lock:
            self._tokens.clear()


class _SupervisedIO:
    """Process-wide singleton owning the supervised stdio channel AND the
    per-audience token cache — writes `auth.token` requests to stdout, one
    background thread reads stdin for the whole process's lifetime and
    dispatches responses by request id.

    Cache lives HERE, not per-`SupervisedTokenSource`: if a process
    constructs more than one `Client`, they must share one cache and one
    single-flight lock (same discipline `TokenManager` already documents —
    "concurrent callers can't stampede" — extended process-wide since
    every `Client` in one process shares the same one supervising parent
    anyway; a second independent ask would just be a wasted round trip).

    Two independent readers on the same stdin would race/corrupt/steal each
    other's line — the reason the dispatcher itself is a singleton too.
    Concurrent asks for DIFFERENT audiences still serialize (one lock
    covers the whole cache, matching TokenManager) — supervised asks are
    low-rate, so that's a fine trade against the alternative of racing the
    same audience's refresh.

    Line-sniffing matches the parent's own strict rule (docs/plans/
    PLAN-EXEC-IDENTITY.md step 3): first byte `{`, parses as JSON, carries
    `jsonrpc: "2.0"` — anything else is the child's own stdin traffic (rare,
    but a script could read stdin itself) and is silently dropped, never
    raised as a protocol error. A single malformed/undecodable line must
    never kill the reader thread — every future `ask()` would then hang
    to its own timeout with no way to ever succeed again."""

    _TIMEOUT = 5.0

    _instance = None
    _instance_lock = threading.Lock()

    def __init__(self):
        self._write_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._pending = {}  # request id -> queue.Queue(maxsize=1)
        self._next_id = 0
        self._reader_started = False
        self._cache_lock = threading.Lock()
        self._tokens = {}  # audience -> (token, expires_at)

    @classmethod
    def instance(cls):
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def _ensure_reader(self):
        if self._reader_started:
            return
        self._reader_started = True
        threading.Thread(target=self._read_loop, daemon=True,
                          name="synthigy-supervised-stdin").start()

    def _read_loop(self):
        while True:
            try:
                line = sys.stdin.readline()
            except Exception:
                # Undecodable bytes, a closed/torn-down stream, whatever —
                # treated exactly like EOF: there is no listener anymore.
                line = ""
            if not line:
                with self._pending_lock:
                    waiters = list(self._pending.values())
                    self._pending.clear()
                for q in waiters:
                    q.put(None)
                return
            try:
                stripped = line.strip()
                if not stripped or stripped[0] != "{":
                    continue
                msg = json.loads(stripped)
                if msg.get("jsonrpc") != "2.0":
                    continue
                with self._pending_lock:
                    q = self._pending.pop(msg.get("id"), None)
                if q is not None:
                    q.put(msg)
            except Exception:
                # A malformed candidate line must not take the dispatcher
                # down with it — keep reading.
                continue

    def _ask(self, method, params, timeout):
        """Send one JSON-RPC request, block up to `timeout` seconds for the
        matching response. Returns the parsed message, or None on timeout,
        parent EOF, or a write failure (broken pipe — parent already gone)."""
        self._ensure_reader()
        with self._pending_lock:
            self._next_id += 1
            req_id = self._next_id
            q = queue.Queue(maxsize=1)
            self._pending[req_id] = q
        frame = json.dumps(
            {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params},
            separators=(",", ":"))
        try:
            # Atomic single write + newline (PLAN-EXEC-IDENTITY step 3's
            # frame rule) — the write lock only serializes concurrent asks
            # against each other, it is not held during the wait below.
            with self._write_lock:
                sys.stdout.write(frame + "\n")
                sys.stdout.flush()
        except Exception:
            with self._pending_lock:
                self._pending.pop(req_id, None)
            return None
        try:
            return q.get(timeout=timeout)
        except queue.Empty:
            with self._pending_lock:
                self._pending.pop(req_id, None)
            return None

    def get_token(self, audience=None):
        key = audience or ""
        with self._cache_lock:
            cached = self._tokens.get(key)
            if cached and time.time() < cached[1] - 30:
                return cached[0]

            params = {"audience": audience} if audience else {}
            msg = self._ask("auth.token", params, self._TIMEOUT)

            if msg is None:
                raise SynthigyError(
                    "Timed out waiting for auth.token from the supervising "
                    "parent — a hung or missing parent must not hang the "
                    "bot. Check the parent process (synthigy exec/agent, "
                    "or the robotics commander) is still connected.",
                    "NO_TOKEN")
            if msg.get("error"):
                err = msg["error"] or {}
                raise SynthigyError(
                    err.get("message") or "auth.token request denied",
                    "NO_TOKEN")
            result = msg.get("result") or {}
            token = result.get("token")
            if not token:
                raise SynthigyError(
                    "auth.token response carried no token", "NO_TOKEN")
            try:
                expires_in = float(result.get("expires_in", 300))
            except (TypeError, ValueError):
                expires_in = 300
            self._tokens[key] = (token, time.time() + expires_in)
            return token

    def clear(self):
        with self._cache_lock:
            self._tokens.clear()


class SupervisedTokenSource:
    """Token source for SYNTHIGY_SUPERVISED=1 — asks `auth.token` over the
    process's own stdio instead of minting locally (the CLI/commander is
    the platform's stdio owner; this SDK never mints, per PLAN-EXEC-IDENTITY
    step 2). Same public shape as TokenManager (`get_token`/`clear`) so
    call sites don't care which source is installed — a thin per-Client
    handle onto the real, process-wide `_SupervisedIO` singleton (cache and
    all), so multiple `Client`s in one process share one cache instead of
    each asking the parent independently.

    `{token, expires_in}` — deliberately byte-compatible with robotics'
    `request-access-token` response, so this ask serves both parents (exec
    locally, the commander through a reacher agent in production)."""

    def __init__(self):
        self._io = _SupervisedIO.instance()

    def get_token(self, audience=None):
        return self._io.get_token(audience)

    def clear(self):
        self._io.clear()


def no_token_error():
    """The teaching throw — PLAN-EXEC-IDENTITY step 3: the error IS the UX,
    no flag, no silent anonymous fallback."""
    return SynthigyError(
        "no Synthigy token: set SYNTHIGY_TOKEN, or SYNTHIGY_CLIENT_ID + "
        "SYNTHIGY_CLIENT_SECRET, or run under `synthigy exec` (or a "
        "Synthigy agent) so a parent can supply one.",
        "NO_TOKEN")
