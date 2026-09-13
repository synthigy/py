"""Connection-pool tests. The main matrix in test_client.py runs against
an HTTP/1.0 stub (server closes every connection) — that exercises the
will_close path for free. Here: keep-alive reuse and stale-conn retry."""

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from synthigy import Client, SynthigyError


class _KeepAliveStub(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive by default

    def log_message(self, *a):
        pass

    def setup(self):
        super().setup()
        with self.server.lock:
            self.server.connections += 1

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n:
            self.rfile.read(n)
        with self.server.lock:
            self.server.hits += 1
            close_now = self.server.hits in self.server.close_on
        body = json.dumps({"results": [{"ok": True, "data": []}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        if close_now:
            self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        if close_now:
            self.close_connection = True


class TestPool(unittest.TestCase):
    def setUp(self):
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _KeepAliveStub)
        self.httpd.lock = threading.Lock()
        self.httpd.connections = 0
        self.httpd.hits = 0
        self.httpd.close_on = set()
        threading.Thread(
            target=lambda: self.httpd.serve_forever(poll_interval=0.05),
            daemon=True).start()
        self.addCleanup(self.httpd.shutdown)
        self.addCleanup(self.httpd.server_close)
        self.endpoint = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def test_sequential_requests_reuse_one_connection(self):
        c = Client(self.endpoint, token="tok")
        for _ in range(4):
            c.search("User", None, None)
        self.assertEqual(self.httpd.hits, 4)
        self.assertEqual(self.httpd.connections, 1)

    def test_server_close_header_drops_conn_and_recovers(self):
        self.httpd.close_on = {2}  # server closes after the 2nd response
        c = Client(self.endpoint, token="tok")
        for _ in range(4):
            c.search("User", None, None)
        self.assertEqual(self.httpd.hits, 4)
        self.assertEqual(self.httpd.connections, 2)  # one reconnect, no error

    def test_client_close_drops_pooled_connections(self):
        c = Client(self.endpoint, token="tok")
        c.search("User", None, None)
        c.close()
        c2 = Client(self.endpoint, token="tok")
        c2.search("User", None, None)
        self.assertEqual(self.httpd.connections, 2)

    def test_concurrent_requests_are_safe_and_bounded(self):
        # parallel callers on the facade's loop thread must each get a
        # correct response; connections never exceed one per concurrent
        # caller
        c = Client(self.endpoint, token="tok")
        errors = []

        def worker():
            try:
                for _ in range(10):
                    assert c.search("User", None, None) == []
            except Exception as e:  # noqa: BLE001 — collected for assertion
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        self.assertEqual(errors, [])
        self.assertEqual(self.httpd.hits, 80)
        self.assertLessEqual(self.httpd.connections, 8)

    def test_connect_refused_is_network_error(self):
        c = Client("http://127.0.0.1:1", token="tok", timeout=0.2)
        with self.assertRaises(SynthigyError) as cm:
            c.search("User", None, None)
        self.assertEqual(cm.exception.code, "NETWORK_ERROR")
        self.assertTrue(cm.exception.retryable)


if __name__ == "__main__":
    unittest.main()
