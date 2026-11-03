"""client.files against a stdlib http.server stub: request shapes, bytes, error mapping."""

import io
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from synthigy import Client, SynthigyError


class _FilesStub(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    protocol_version = "HTTP/1.1"

    def _send(self, status, body=b"", headers=None):
        self.send_response(status)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        if status not in (204, 304):  # as http-kit: no length on a bodiless answer
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status, payload):
        self._send(status, json.dumps(payload).encode(), {"Content-Type": "application/json"})

    def _handle(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else b""
        self.server.calls.append({"method": self.command, "path": self.path,
                                  "headers": dict(self.headers), "body": body})
        path, _, query = self.path.partition("?")
        if path == "/files" and self.command in ("POST", "PUT"):
            self._json(200, {"xid": "f1", "name": "a.txt", "size": len(body), "hash": "h1",
                             "uploaded_by": {"xid": "u1", "name": "jane"}, "folder": None})
        elif path in ("/files", "/files/f1") and self.command == "PATCH":
            self._json(200, {"xid": "f1", "name": "n.png", "folder": {"xid": "d1", "path": "/b/"}})
        elif path == "/files/info" and "gone" in query:
            self._json(404, {"error": {"code": "FILE_NOT_FOUND", "message": "No such file"}})
        elif path == "/files/info":
            self._json(200, {"xid": "f9", "name": "r.txt"})
        elif path == "/files" and self.command == "GET" and "gone" in query:
            self._json(404, {"error": {"code": "FILE_NOT_FOUND", "message": "No such file"}})
        elif path == "/files" and self.command == "GET" and "path=" in query:
            self._send(206, b"he", {"ETag": '"h1"', "Content-Range": "bytes 0-1/5", "X-Internal": "no"})
        elif path == "/files":
            self._json(200, {"files": [{"xid": "f1", "name": "a.txt"}]})
        elif path == "/files/search" and "q=+" in query:
            self._json(400, {"error": {"code": "INVALID_QUERY", "message": "q"}})
        elif path == "/files/search":
            self._json(200, {"truncated": True, "results": [
                {"type": "folder", "xid": "d1", "name": "Sub Two", "path": "/a/Sub Two/"},
                {"type": "file", "xid": "f1", "name": "x.bin", "path": None}]})
        elif path == "/files/f1" and self.command == "PUT":
            self._json(412, {"error": {"code": "FILE_CHANGED", "message": "changed"}})
        elif path in ("/files/f1", "/files/f1/versions/7", "/files/f9/versions/7") and self.command == "GET":
            if self.headers.get("If-None-Match") == '"h1"':
                self._send(304, headers={"ETag": '"h1"'})
            elif self.headers.get("Range"):
                self._send(206, b"234", {"ETag": '"h1"', "Content-Range": "bytes 2-4/10"})
            else:
                self._send(200, b"0123456789", {"ETag": '"h1"'})
        elif path == "/files/f2/info":
            self._json(404, {"error": {"code": "FILE_NOT_FOUND", "message": "No such file"}})
        elif path == "/files/f3/info":
            self._json(503, {"error": {"code": "AUDIT_UNAVAILABLE", "message": "down"}})
        elif path.endswith("/versions"):
            self._json(200, {"xid": "f1", "versions": [{"id": 7, "at": "2026-10-29T17:40:12.532Z",
                                                         "by": "u1", "current": True}]})
        elif path.endswith("/restore"):
            self._json(200, {"xid": "f1"})
        elif path == "/files/folders" and self.command == "GET":
            self._json(200, {"parent": "root", "folders": [{"xid": "d1", "name": "Sub Two", "path": "/a/Sub Two/"},
                                                           {"xid": "d2", "name": "other", "path": "/a/other/"}]})
        elif path.endswith("/share") or path.endswith("/unshare") or path.endswith("/access"):
            self._json(200, {"read": {"users": [{"xid": "u2", "name": "bob"}], "groups": []},
                             "write": {"users": [], "groups": []}, "delete": {"users": [], "groups": []}})
        elif self.command == "DELETE":
            self._send(204)
        elif path.startswith("/files/folders"):
            self._json(200, {"xid": "d3", "name": "new", "path": "/b/new/", "public": True, "cache": None,
                             "settings": {"public": True, "cache": None}})
        else:
            self._send(404, b"Not Found")

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = _handle


class FilesTest(unittest.TestCase):
    def setUp(self):
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _FilesStub)
        self.httpd.calls = []
        threading.Thread(target=lambda: self.httpd.serve_forever(poll_interval=0.05), daemon=True).start()
        self.addCleanup(self.httpd.server_close)
        self.addCleanup(self.httpd.shutdown)
        self.c = Client(f"http://127.0.0.1:{self.httpd.server_address[1]}", token="tok")
        self.addCleanup(self.c.close)

    @property
    def last(self):
        return self.httpd.calls[-1]

    def code(self, fn):
        with self.assertRaises(SynthigyError) as cm:
            fn()
        return cm.exception

    def body(self):
        return json.loads(self.last["body"])

    def test_put(self):
        fi = self.c.files.put("/docs/a b.txt", b"hello")
        self.assertEqual((fi["xid"], fi["size"], fi["uploaded_by"]["name"]), ("f1", 5, "jane"))
        self.assertEqual((self.last["method"], self.last["path"]), ("PUT", "/files?path=%2Fdocs%2Fa+b.txt"))
        self.assertEqual(self.last["headers"]["Content-Type"], "application/octet-stream")
        self.assertEqual(self.last["headers"]["Authorization"], "Bearer tok")

        self.c.files.put({"name": "loose.txt"}, io.BytesIO(b"from a file"))
        self.assertEqual((self.last["method"], self.last["path"], self.last["body"]),
                         ("POST", "/files?name=loose.txt", b"from a file"))
        self.c.files.put("/t.txt", "héllo")
        self.assertEqual(self.last["headers"]["Content-Type"], "text/plain; charset=utf-8")
        self.assertEqual(self.last["body"], "héllo".encode())

        e = self.code(lambda: self.c.files.put({"xid": "f1"}, b"v2", if_match={"hash": "abc"}))
        self.assertEqual((e.code, e.category, e.status), ("FILE_CHANGED", "conflict", 412))
        self.assertEqual(self.last["headers"]["If-Match"], '"abc"')
        self.assertEqual(self.code(lambda: self.c.files.put({}, b"x")).code, "INVALID_PATH")
        e = self.code(lambda: self.c.files.put("/x", b"a=b", content_type="application/x-www-form-urlencoded"))
        self.assertEqual(e.code, "UNSUPPORTED_MEDIA_TYPE")

    def test_info_and_errors(self):
        self.assertIsNone(self.c.files.info("f2"))
        self.assertIsNone(self.c.files.info("/gone.txt"))
        self.assertEqual(self.last["path"], "/files/info?path=%2Fgone.txt")
        e = self.code(lambda: self.c.files.info("f3"))
        self.assertEqual((e.code, e.category, e.retryable), ("AUDIT_UNAVAILABLE", "not_found", False))
        e = self.code(lambda: self.c.files.search(" "))
        self.assertEqual((e.code, e.category), ("INVALID_QUERY", "validation"))
        self.assertEqual(self.code(lambda: self.c.files.download("zz")).code, "FILES_UNAVAILABLE")
        self.assertEqual(self.code(lambda: self.c.files.versions("/gone.txt")).code, "FILE_NOT_FOUND")

    def test_download(self):
        self.assertEqual(self.c.files.download("f1"), b"0123456789")
        self.assertEqual(self.c.files.download("f1", byte_range=(2, 4)), b"234")
        self.assertEqual(self.last["headers"]["Range"], "bytes=2-4")
        self.c.files.download("f1", byte_range=(None, 5))
        self.assertEqual(self.last["headers"]["Range"], "bytes=-5")
        self.assertIsNone(self.c.files.download("f1", if_none_match="h1"))
        self.c.files.download("/a/r.txt", version={"id": 7})
        self.assertEqual([c["path"] for c in self.httpd.calls[-2:]],
                         ["/files/info?path=%2Fa%2Fr.txt", "/files/f9/versions/7"])

    def test_list_search_move(self):
        self.assertEqual(self.c.files.list(limit=5, offset=10)["folders"], [])
        self.assertEqual(self.last["path"], "/files?limit=5&offset=10")
        self.c.files.list(None, q="inv")
        self.assertEqual(self.last["path"], "/files?q=inv&folderless=true")
        ls = self.c.files.list("/a/", q="sub two")
        self.assertEqual([f["xid"] for f in ls["folders"]], ["d1"])
        self.assertEqual([c["path"] for c in self.httpd.calls[-2:]],
                         ["/files?q=sub+two&folder=%2Fa%2F", "/files/folders?parent=%2Fa%2F"])

        hits = self.c.files.search("sub x", folder={"xid": "d0", "path": "/a/"}, limit=2)
        self.assertEqual(self.last["path"], "/files/search?q=sub+x&folder=%2Fa%2F&limit=2")
        self.assertEqual(([r["type"] for r in hits["results"]], hits["truncated"]), (["folder", "file"], True))

        self.assertEqual(self.c.files.move("/a/x.png", "/b/n.png")["folder"]["path"], "/b/")
        self.assertEqual((self.last["method"], self.last["path"], self.body()),
                         ("PATCH", "/files?path=%2Fa%2Fx.png", {"folder": "/b/", "name": "n.png"}))
        self.c.files.move({"xid": "f1"}, "/b/")
        self.assertEqual((self.last["path"], self.body()), ("/files/f1", {"folder": "/b/"}))
        self.assertEqual(self.c.files.move("/a/old/", "/b/new/")["path"], "/b/new/")
        self.assertEqual((self.last["path"], self.body()),
                         ("/files/folders?path=%2Fa%2Fold%2F", {"name": "new", "parent": "/b/"}))
        self.c.files.move({"xid": "d9", "path": "/a/old/"}, "/top/")
        self.assertEqual((self.last["path"], self.body()), ("/files/folders?path=%2Fa%2Fold%2F", {"name": "top", "parent": "/"}))
        self.assertEqual(self.code(lambda: self.c.files.move("f1", "n.png")).code, "INVALID_PATH")

    def test_folders_and_sharing(self):
        d = self.c.files.folder("/a b/", public=True, cache=None)
        self.assertEqual((d["public"], d["settings"]), (True, {"public": True, "cache": None}))
        self.assertEqual((self.last["method"], self.last["path"], self.body()),
                         ("PUT", "/files/folders?path=%2Fa+b%2F", {"public": True, "cache": None}))
        self.c.files.folder("/x/")
        self.assertEqual(self.body(), {})
        self.assertIsNone(self.c.files.share("/a b/", "read", users=[{"xid": "u2"}]))
        self.assertEqual((self.last["path"], self.body()),
                         ("/files/folders/share?path=%2Fa+b%2F", {"access": "read", "users": ["u2"], "groups": []}))
        self.c.files.unshare("d1", "read", groups=["g1"])
        self.assertEqual(self.last["path"], "/files/folders/d1/unshare")
        self.assertEqual(self.c.files.access("/a b/")["read"]["users"][0]["name"], "bob")
        self.assertEqual(self.last["path"], "/files/folders/access?path=%2Fa+b%2F")
        vs = self.c.files.versions("f1")
        self.assertEqual((vs[0]["id"], vs[0]["current"]), (7, True))
        self.c.files.restore("f1", vs[0])
        self.assertEqual((self.last["method"], self.last["path"]), ("POST", "/files/f1/versions/7/restore"))
        self.c.files.delete("/a b/r.txt")
        self.assertEqual((self.last["method"], self.last["path"]), ("DELETE", "/files?path=%2Fa+b%2Fr.txt"))
        self.c.files.delete("/a b/")
        self.assertEqual(self.last["path"], "/files/folders?path=%2Fa+b%2F")
        self.c.files.delete({"xid": "f1"})
        self.assertEqual(self.last["path"], "/files/f1")

    def test_url_and_serve(self):
        n = len(self.httpd.calls)
        base = self.c._config["endpoint"]
        self.assertEqual(self.c.files.url("/site/a b(1)*.txt"), f"{base}/files/path/site/a%20b%281%29%2A.txt")
        self.assertEqual(self.c.files.url({"xid": "f1"}, inline=True), f"{base}/files/f1?disposition=inline")
        self.assertEqual(len(self.httpd.calls), n, "url makes no request")
        self.assertEqual(self.code(lambda: self.c.files.url("/a/")).code, "INVALID_PATH")

        status, headers, body = self.c.files.serve("/note.txt", {"Range": "bytes=0-1", "Cookie": "x"}, inline=True)
        self.assertEqual((status, body, headers.get("etag"), headers.get("x-internal")), (206, b"he", '"h1"', None))
        self.assertEqual(self.last["path"], "/files?path=%2Fnote.txt&disposition=inline")
        self.assertEqual((self.last["headers"].get("Range"), self.last["headers"].get("Cookie")), ("bytes=0-1", None))
        status, _h, body = self.c.files.serve("/gone.txt")
        self.assertEqual(status, 404)
        self.assertIn(b"FILE_NOT_FOUND", body)


if __name__ == "__main__":
    unittest.main()
