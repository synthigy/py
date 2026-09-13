"""Contract test for the supervised-stdio token source — spawns the SDK's
own process under a stub parent speaking `auth.token`, per
docs/plans/PLAN-EXEC-IDENTITY.md steps 3-4. The child follows the
supervised convention (logs/results to stderr, stdout is exclusively the
control channel) so this test's own bookkeeping never collides with the
protocol it's exercising."""

import json
import os
import subprocess
import sys
import unittest

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CHILD_SCRIPT = r"""
import asyncio, json, sys
import synthigy

async def main():
    client = synthigy.AsyncClient("http://unused.invalid")
    try:
        token = await client.token()
        print(json.dumps({"ok": True, "token": token}), file=sys.stderr, flush=True)
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e), "code": getattr(e, "code", None)}),
              file=sys.stderr, flush=True)

asyncio.run(main())
"""


def _spawn_child():
    env = dict(os.environ)
    env["SYNTHIGY_SUPERVISED"] = "1"
    for k in ("SYNTHIGY_TOKEN", "SYNTHIGY_CLIENT_ID", "SYNTHIGY_CLIENT_SECRET"):
        env.pop(k, None)
    env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.Popen(
        [sys.executable, "-c", CHILD_SCRIPT],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=env, text=True, bufsize=1)


class SupervisedAuthTest(unittest.TestCase):
    def test_asks_and_receives_token(self):
        proc = _spawn_child()
        try:
            line = proc.stdout.readline()
            self.assertTrue(line, "child never wrote a frame to stdout")
            frame = json.loads(line)
            self.assertEqual(frame.get("jsonrpc"), "2.0")
            self.assertEqual(frame.get("method"), "auth.token")
            self.assertIn("id", frame)

            response = json.dumps({
                "jsonrpc": "2.0", "id": frame["id"],
                "result": {"token": "supervised-token-abc", "expires_in": 300},
            }) + "\n"
            proc.stdin.write(response)
            proc.stdin.flush()

            result = json.loads(proc.stderr.readline())
        finally:
            proc.stdin.close()
            proc.wait(timeout=5)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["token"], "supervised-token-abc")

    def test_stray_and_mismatched_lines_are_ignored(self):
        """Non-frame noise and a response for a DIFFERENT request id on the
        same stdin must not corrupt dispatch of the real response."""
        proc = _spawn_child()
        try:
            frame = json.loads(proc.stdout.readline())
            proc.stdin.write("not json at all\n")
            proc.stdin.write(json.dumps(
                {"jsonrpc": "2.0", "id": frame["id"] + 999,
                 "result": {"token": "wrong-request"}}) + "\n")
            proc.stdin.write(json.dumps({
                "jsonrpc": "2.0", "id": frame["id"],
                "result": {"token": "right-token", "expires_in": 300},
            }) + "\n")
            proc.stdin.flush()
            result = json.loads(proc.stderr.readline())
        finally:
            proc.stdin.close()
            proc.wait(timeout=5)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["token"], "right-token")

    def test_timeout_falls_through_to_the_teaching_throw(self):
        """A hung or missing parent must not hang the bot — see step 3."""
        proc = _spawn_child()
        try:
            self.assertTrue(proc.stdout.readline())
            # Never respond.
            result = json.loads(proc.stderr.readline())
        finally:
            proc.stdin.close()
            proc.wait(timeout=10)
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "NO_TOKEN")

    def test_pipe_beats_env_token_when_supervised(self):
        """SYNTHIGY_TOKEN in env AND SYNTHIGY_SUPERVISED=1: the pipe wins —
        exec injects the cached env token and supervises, and only the pipe
        can refresh mid-run (the env value is a frozen snapshot)."""
        env = dict(os.environ)
        env["SYNTHIGY_SUPERVISED"] = "1"
        env["SYNTHIGY_TOKEN"] = "stale-env-snapshot"
        for k in ("SYNTHIGY_CLIENT_ID", "SYNTHIGY_CLIENT_SECRET"):
            env.pop(k, None)
        env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.Popen(
            [sys.executable, "-c", CHILD_SCRIPT],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=env, text=True, bufsize=1)
        try:
            line = proc.stdout.readline()
            self.assertTrue(line, "supervised child must ask the pipe even "
                                   "with SYNTHIGY_TOKEN set")
            frame = json.loads(line)
            proc.stdin.write(json.dumps({
                "jsonrpc": "2.0", "id": frame["id"],
                "result": {"token": "fresh-pipe-token", "expires_in": 300},
            }) + "\n")
            proc.stdin.flush()
            result = json.loads(proc.stderr.readline())
        finally:
            proc.stdin.close()
            proc.wait(timeout=5)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["token"], "fresh-pipe-token")

    def test_malformed_line_does_not_kill_the_reader(self):
        """A garbage line between two independent asks must not take the
        background dispatcher down with it — the second ask still has to
        work, or every future request in this process hangs forever."""
        proc = _spawn_child_two_asks()
        try:
            frame1 = json.loads(proc.stdout.readline())
            proc.stdin.write("\xff not even valid json {\n")
            proc.stdin.write("{ this looks like a frame but isn't valid json\n")
            proc.stdin.write(json.dumps({
                "jsonrpc": "2.0", "id": frame1["id"],
                "result": {"token": "first-token", "expires_in": 300},
            }) + "\n")
            proc.stdin.flush()

            frame2 = json.loads(proc.stdout.readline())
            self.assertNotEqual(frame2["id"], frame1["id"])
            proc.stdin.write(json.dumps({
                "jsonrpc": "2.0", "id": frame2["id"],
                "result": {"token": "second-token", "expires_in": 300},
            }) + "\n")
            proc.stdin.flush()

            result = json.loads(proc.stderr.readline())
        finally:
            proc.stdin.close()
            proc.wait(timeout=5)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["tokens"], ["first-token", "second-token"])


CHILD_SCRIPT_TWO_ASKS = r"""
import asyncio, json, sys
from synthigy.auth import SupervisedTokenSource

async def main():
    src = SupervisedTokenSource()
    try:
        t1 = src.get_token("aud-a")
        t2 = src.get_token("aud-b")
        print(json.dumps({"ok": True, "tokens": [t1, t2]}), file=sys.stderr, flush=True)
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e), "code": getattr(e, "code", None)}),
              file=sys.stderr, flush=True)

asyncio.run(main())
"""


def _spawn_child_two_asks():
    env = dict(os.environ)
    env["SYNTHIGY_SUPERVISED"] = "1"
    for k in ("SYNTHIGY_TOKEN", "SYNTHIGY_CLIENT_ID", "SYNTHIGY_CLIENT_SECRET"):
        env.pop(k, None)
    env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
    return subprocess.Popen(
        [sys.executable, "-c", CHILD_SCRIPT_TWO_ASKS],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=env, text=True, bufsize=1)


class _Stub401Once(BaseHTTPRequestHandler):
    server_version = "stub"

    def log_message(self, *a):  # silence
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(n)
        self.server.auth_headers.append(self.headers.get("Authorization"))
        if len(self.server.auth_headers) == 1:
            body = json.dumps(
                {"error": {"message": "Unauthorized", "code": "UNAUTHORIZED"}}
            ).encode()
            self.send_response(401)
        else:
            body = json.dumps({"results": [{"ok": True, "data": []}]}).encode()
            self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


CHILD_SCRIPT_END_TO_END = r"""
import asyncio, json, sys
import synthigy

async def main():
    client = synthigy.AsyncClient(sys.argv[1])
    try:
        await client.search("User", None, None)
        print(json.dumps({"ok": True}), file=sys.stderr, flush=True)
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e), "code": getattr(e, "code", None)}),
              file=sys.stderr, flush=True)

asyncio.run(main())
"""


class SupervisedAuthEndToEndTest(unittest.TestCase):
    def test_401_clears_the_cache_and_reasks_the_parent(self):
        """The full lifecycle a real bot exercises: ask, use, get 401
        (token was revoked/invalid despite our clock saying it's fresh),
        clear, ask again, retry succeeds with the NEW token — proving
        SupervisedTokenSource.clear() is actually wired into the SDK's
        existing 401 clear+retry-once path, not just present."""
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Stub401Once)
        httpd.auth_headers = []
        thread = __import__("threading").Thread(
            target=lambda: httpd.serve_forever(poll_interval=0.05), daemon=True)
        thread.start()
        endpoint = f"http://127.0.0.1:{httpd.server_address[1]}"

        env = dict(os.environ)
        env["SYNTHIGY_SUPERVISED"] = "1"
        for k in ("SYNTHIGY_TOKEN", "SYNTHIGY_CLIENT_ID", "SYNTHIGY_CLIENT_SECRET"):
            env.pop(k, None)
        env["PYTHONPATH"] = REPO_ROOT + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.Popen(
            [sys.executable, "-c", CHILD_SCRIPT_END_TO_END, endpoint],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=env, text=True, bufsize=1)
        try:
            frame1 = json.loads(proc.stdout.readline())
            proc.stdin.write(json.dumps({
                "jsonrpc": "2.0", "id": frame1["id"],
                "result": {"token": "stale-token", "expires_in": 300},
            }) + "\n")
            proc.stdin.flush()

            frame2 = json.loads(proc.stdout.readline())
            self.assertNotEqual(frame2["id"], frame1["id"],
                                 "the 401 must trigger a SECOND, independent ask")
            proc.stdin.write(json.dumps({
                "jsonrpc": "2.0", "id": frame2["id"],
                "result": {"token": "fresh-token", "expires_in": 300},
            }) + "\n")
            proc.stdin.flush()

            result = json.loads(proc.stderr.readline())
        finally:
            proc.stdin.close()
            proc.wait(timeout=5)
            httpd.shutdown()
            httpd.server_close()

        self.assertTrue(result["ok"], result)
        self.assertEqual(httpd.auth_headers,
                          ["Bearer stale-token", "Bearer fresh-token"])


if __name__ == "__main__":
    unittest.main()
