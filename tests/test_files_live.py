"""Live files suite: client.files and AsyncClient.files against a real engine
with the files module, as three principals (File Admin, File User, File Reader).

Skipped unless SYNTHIGY_FILES_TEST_ENDPOINT is set. Each principal is a
client-credentials client whose service user holds the matching role:

    SYNTHIGY_FILES_TEST_ENDPOINT
    SYNTHIGY_FILES_TEST_ADMIN_ID   / SYNTHIGY_FILES_TEST_ADMIN_SECRET
    SYNTHIGY_FILES_TEST_USER_ID    / SYNTHIGY_FILES_TEST_USER_SECRET
    SYNTHIGY_FILES_TEST_READER_ID  / SYNTHIGY_FILES_TEST_READER_SECRET
    SYNTHIGY_FILES_TEST_BFF_ID     / SYNTHIGY_FILES_TEST_BFF_SECRET  (trusted, File Admin)

Run: python3 -m unittest tests.test_files_live -v
"""

import asyncio
import os
import time
import unittest
import urllib.error
import urllib.request
import uuid

from synthigy import AsyncClient, Client, SynthigyError

ENDPOINT = os.environ.get("SYNTHIGY_FILES_TEST_ENDPOINT")
ROLES = ("ADMIN", "USER", "READER", "BFF")


def _creds(role):
    return (os.environ.get(f"SYNTHIGY_FILES_TEST_{role}_ID"),
            os.environ.get(f"SYNTHIGY_FILES_TEST_{role}_SECRET"))


def _client(role):
    cid, secret = _creds(role)
    return Client(ENDPOINT, client_id=cid, client_secret=secret, timeout=30)


def _quiet(fn, *a):
    try:
        fn(*a)
    except Exception:
        pass


BINARY = bytes([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0xff, 0xfe, 0x80, 0x00])
NOT_UTF8_JSON = bytes([0xff, 0xfe, 0x7b, 0x7d])


@unittest.skipUnless(ENDPOINT and all(all(_creds(r)) for r in ROLES),
                     "set SYNTHIGY_FILES_TEST_ENDPOINT and the ADMIN/USER/READER/BFF client pairs")
class TestFilesLive(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin, cls.user, cls.reader, cls.bff = (_client(r) for r in ROLES)

    @classmethod
    def tearDownClass(cls):
        for c in (cls.admin, cls.user, cls.reader, cls.bff):
            c.close()

    def setUp(self):
        self.run_id = uuid.uuid4().hex[:10]
        self.top = f"/F-py-{self.run_id}/"
        self.files = []
        self.folders = []

    def tearDown(self):
        for xid in self.files:
            _quiet(self.admin.files.delete, xid)
        for path in reversed(self.folders):
            _quiet(self.admin.files.delete, path)

    def name(self, label):
        return f"py-{self.run_id}-{label}"

    def assertError(self, code, fn, *a, category=None, status=None, **kw):
        with self.assertRaises(SynthigyError) as cm:
            fn(*a, **kw)
        self.assertEqual(cm.exception.code, code, cm.exception.message)
        if category:
            self.assertEqual(cm.exception.category, category)
        if status:
            self.assertEqual(getattr(cm.exception, "status", None), status)
        return cm.exception

    def put(self, client, target, data, **kw):
        f = client.files.put(target, data, **kw)
        if f["xid"] not in self.files:
            self.files.append(f["xid"])
        return f

    def status(self, url):
        try:
            with urllib.request.urlopen(url) as r:
                return r.status
        except urllib.error.HTTPError as e:
            return e.code

    def test_scenario(self):
        admin, user, reader = self.admin, self.user, self.reader
        top, run = self.top, self.run_id

        # 1. admin makes the top folder and shares it
        self.assertEqual(admin.files.folder(top)["path"], top)
        self.folders.append(top)
        user_xid = user.whoami()["xid"]
        reader_xid = reader.whoami()["xid"]
        for access in ("write", "read", "delete"):
            admin.files.share(top, access, users=[user_xid])
        admin.files.share(top, "read", users=[reader_xid])
        acl = admin.files.access(top)
        holders = {k: {u["xid"] for u in acl[k]["users"]} for k in ("read", "write", "delete")}
        self.assertTrue({user_xid, reader_xid} <= holders["read"])
        self.assertIn(user_xid, holders["write"])
        self.assertIn(user_xid, holders["delete"])
        self.assertNotIn(reader_xid, holders["write"])

        # 2. user puts, replaces and versions
        bin_path = f"{top}{self.name('bin.dat')}"
        f = self.put(user, bin_path, BINARY)
        self.assertEqual(f["size"], len(BINARY))
        self.assertEqual(user.files.download(bin_path), BINARY)
        changed = BINARY + b"\x00v2\xff"
        f2 = user.files.put(bin_path, changed)
        self.assertEqual(f2["xid"], f["xid"])
        self.assertNotEqual(f2["hash"], f["hash"])
        self.assertError("FILE_CHANGED", user.files.put, f, BINARY, if_match="0" * 64, category="conflict")
        self.assertError("FOLDER_NOT_FOUND", user.files.put, f"{top}nope/x.bin", BINARY, category="not_found")
        self.assertError("INVALID_NAME", user.files.put, f"{top}a\\b", BINARY, category="validation")

        deadline = time.monotonic() + 30
        versions = user.files.versions(bin_path)
        while len(versions) < 2 and time.monotonic() < deadline:
            time.sleep(0.5)
            versions = user.files.versions(bin_path)
        self.assertEqual(len(versions), 2)
        self.assertTrue(versions[0]["current"])
        self.assertEqual(versions[1]["hash"], f["hash"])
        self.assertEqual(user.files.download(f, version=versions[1]), BINARY)
        self.assertEqual(user.files.restore(bin_path, versions[1])["hash"], f["hash"])
        self.assertEqual(user.files.download(f), BINARY)
        self.assertEqual(user.files.download(bin_path, byte_range=(2, 4)), BINARY[2:5])
        self.assertEqual(user.files.download(f, byte_range=(None, 3)), BINARY[-3:])
        self.assertIsNone(user.files.download(f, if_none_match=f["hash"]))

        j = self.put(user, f"{top}{self.name('raw.json')}", NOT_UTF8_JSON, content_type="application/json")
        self.assertEqual(user.files.download(j), NOT_UTF8_JSON)
        self.assertEqual(user.files.info(bin_path)["xid"], f["xid"])
        self.assertEqual(user.files.info(f["xid"])["folder"]["path"], top)
        self.assertIsNone(user.files.info(f"{top}missing.bin"))
        ls = user.files.list(top)
        self.assertEqual({x["xid"] for x in ls["files"]}, {f["xid"], j["xid"]})
        self.assertEqual(ls["folders"], [])
        self.assertEqual([x["xid"] for x in user.files.list(q=f"{run}-bin")["files"]], [f["xid"]])

        loose = self.put(user, {"name": self.name("loose.txt")}, b"folderless")
        self.assertIsNone(loose.get("folder"))
        twin = user.files.put({"name": self.name("loose.txt")}, b"twin")
        self.assertNotEqual(twin["xid"], loose["xid"])
        user.files.delete(twin)
        self.assertEqual([x["xid"] for x in user.files.list(None, q=self.name("loose"))["files"]], [loose["xid"]])

        # 3. user folders and moves
        user.files.folder(f"{top}sub/")
        self.folders.append(f"{top}sub/")
        self.assertEqual(user.files.move(f"{top}sub/", f"{top}sub2/")["path"], f"{top}sub2/")
        self.folders.append(f"{top}sub2/")
        self.assertEqual([d["path"] for d in user.files.list(top)["folders"]], [f"{top}sub2/"])
        self.assertEqual(user.files.move(f, f"{top}sub2/")["folder"]["path"], f"{top}sub2/")
        self.assertEqual(user.files.move(f"{top}sub2/{self.name('bin.dat')}", top)["folder"]["path"], top)
        self.assertError("FORBIDDEN", user.files.move, f"{top}sub2/", f"/sub2-{run}/", category="iam")
        self.assertError("FOLDER_NOT_EMPTY", user.files.delete, top, category="conflict")

        # 3b. search and list q
        hit = user.files.search(f"F-py-{run} sub2")
        self.assertEqual([(r["type"], r["path"]) for r in hit["results"]], [("folder", f"{top}sub2/")])
        self.assertFalse(hit["truncated"])
        hit = user.files.search(f"{run} bin")
        self.assertEqual([(r["type"], r["xid"], r["path"]) for r in hit["results"]], [("file", f["xid"], bin_path)])
        self.assertEqual(user.files.search(f".dat F-py-{run}")["results"], [])
        everything = user.files.search(f"F-py-{run}")["results"]
        self.assertGreaterEqual(len(everything), 4)
        self.assertEqual(everything[0]["path"], top)
        paged, offset, more = [], 0, True
        while more:
            page = user.files.search(f"F-py-{run}", limit=2, offset=offset)
            paged += page["results"]
            more, offset = page["truncated"], offset + 2
        self.assertEqual([r["xid"] for r in paged], [r["xid"] for r in everything])
        self.assertEqual([r["path"] for r in user.files.search("sub2", folder=top)["results"]], [f"{top}sub2/"])
        self.assertEqual([(r["xid"], r["path"]) for r in user.files.search(self.name("loose"))["results"]],
                         [(loose["xid"], None)])
        self.assertEqual(reader.files.search(self.name("loose"))["results"], [])
        self.assertEqual(user.files.list(top, q="%")["files"], [])
        self.assertEqual([d["path"] for d in user.files.list(top, q="sub")["folders"]], [f"{top}sub2/"])
        self.assertError("INVALID_QUERY", user.files.search, "  ", category="validation")

        # 4. reader sees and downloads, may not write
        seen = {d["path"]: d for d in reader.files.list("/")["folders"]}
        self.assertIn(top, seen)
        self.assertFalse(seen[top]["writable"])
        self.assertEqual(reader.files.download(f), BINARY)
        self.assertIsNone(reader.files.info(loose))
        self.assertError("FORBIDDEN", reader.files.put, f"{top}r-{run}.bin", b"x", category="iam")
        self.assertError("FORBIDDEN", reader.files.put, f, b"x", category="iam", status=403)
        self.assertError("FORBIDDEN", reader.files.folder, f"{top}r-{run}/", category="iam")
        self.assertError("FORBIDDEN", reader.files.restore, f, versions[0], category="iam")

        # 4b. a trusted client acting as the user and the reader
        bff = self.bff
        self.assertEqual(bff.files.info(loose)["xid"], loose["xid"])
        self.assertIsNone(bff.files.info(loose, acting_as=reader_xid))
        self.assertEqual(bff.files.download(f, acting_as=reader_xid), BINARY)
        self.assertError("FORBIDDEN", bff.files.put, f"{top}bff-r-{run}.bin", b"x", acting_as=reader_xid, category="iam")
        own = bff.files.put(f"{top}bff-u-{run}.bin", b"x", acting_as=user_xid)
        self.assertEqual(own["uploaded_by"]["xid"], user_xid)
        bff.files.delete(own, acting_as=user_xid)
        self.assertIsNone(bff.files.info(own))
        self.assertError("NOT_TRUSTED", admin.files.info, f, acting_as=reader_xid)

        # 5. links for browsers
        note_path = f"{top}{self.name('note.txt')}"
        note = self.put(user, note_path, "hello")
        self.assertEqual(self.status(user.files.url(note)), 401)
        self.assertError("ATTRIBUTE_FORBIDDEN", user.files.folder, top, public=True, category="iam")
        user.files.folder(top, cache=120)
        shown = admin.files.folder(top, public=True)
        self.assertEqual((shown["public"], shown["cache"], shown["settings"]),
                         (True, 120, {"public": True, "cache": 120, "can_link": None}))
        self.assertTrue(user.files.info(note)["public"])
        for url in (user.files.url(note), user.files.url(note_path)):
            with urllib.request.urlopen(url) as r:
                self.assertEqual(r.read(), b"hello")
                self.assertEqual(r.headers["Cache-Control"], "public, max-age=120")
                self.assertEqual(r.headers["Access-Control-Allow-Origin"], "*")
        with urllib.request.urlopen(user.files.url(note, inline=True)) as r:
            self.assertTrue(r.headers["Content-Disposition"].startswith("inline"))
        inner = admin.files.folder(f"{top}inner/")
        self.folders.append(f"{top}inner/")
        self.assertEqual((inner["public"], inner["settings"]["public"]), (True, None))
        secret = self.put(user, f"{top}inner/{self.name('secret.txt')}", "secret")
        self.assertEqual(self.status(user.files.url(secret)), 200)
        admin.files.folder(f"{top}inner/", public=False)
        self.assertEqual(self.status(user.files.url(secret)), 401)
        admin.files.folder(f"{top}inner/", public=None)
        self.assertEqual(self.status(user.files.url(secret)), 200)
        user.files.delete(secret)
        admin.files.delete(f"{top}inner/")
        self.folders.remove(f"{top}inner/")
        status, headers, body = user.files.serve(note_path, {"Range": "bytes=0-1"})
        self.assertEqual((status, body), (206, b"he"))
        self.assertEqual(user.files.serve(f"{top}gone-{run}.txt")[0], 404)

        # 5b. download links
        shut = self.put(user, f"{top}{self.name('link.txt')}", "linked")
        admin.files.folder(top, public=False)
        self.assertError("LINKS_DISABLED", user.files.link, shut, category="iam")
        self.assertError("ATTRIBUTE_FORBIDDEN", user.files.folder, top, can_link=True, category="iam")
        self.assertTrue(admin.files.folder(top, can_link=True)["can_link"])
        self.assertError("FORBIDDEN", reader.files.link, shut, category="iam")
        self.assertError("INVALID_EXPIRES", user.files.link, shut, expires=0, category="validation")

        link = user.files.link(shut, expires=600)
        self.assertEqual((link["file"], link["created_by"], link["downloads"]), (shut["xid"], user_xid, 0))
        with urllib.request.urlopen(link["url"]) as r:
            self.assertEqual(r.headers["Referrer-Policy"], "no-referrer")
            self.assertEqual(r.read(), b"linked")

        once = user.files.link(shut, max_downloads=1, inline=True)
        with urllib.request.urlopen(urllib.request.Request(once["url"], headers={"Range": "bytes=1-2"})) as r:
            self.assertEqual(r.status, 200, "a limited link ignores Range")
            self.assertTrue(r.headers["Content-Disposition"].startswith("inline"))
            self.assertEqual(r.read(), b"linked")
        self.assertEqual(self.status(once["url"]), 404, "used up")

        cid, secret = _creds("USER")
        with Client(ENDPOINT, client_id=cid, client_secret=secret, timeout=30, key_format="camel") as camel:
            seen = next(x for x in camel.files.links(shut) if x["xid"] == link["xid"])
            self.assertEqual((seen["createdBy"], seen["maxDownloads"], "created_by" in seen), (user_xid, None, False))
            self.assertEqual(camel.files.info(shut)["contentType"], user.files.info(shut)["content_type"])
        mine = user.files.links(shut)
        self.assertTrue(all("url" not in x for x in mine), "the url is shown once")
        self.assertEqual(next(x for x in mine if x["xid"] == link["xid"])["downloads"], 1)
        admins = admin.files.link(shut)
        self.assertNotIn(admins["xid"], {x["xid"] for x in user.files.links(shut)}, "File User sees its own")
        self.assertIn(link["xid"], {x["xid"] for x in admin.files.links(shut)}, "File Admin sees all")
        self.assertError("LINK_NOT_FOUND", user.files.unlink, admins["xid"], category="not_found")

        admin.files.folder(top, can_link=False)
        self.assertEqual(self.status(link["url"]), 404, "switched off: existing links stop")
        admin.files.folder(top, can_link=True)
        self.assertEqual(self.status(link["url"]), 200)
        user.files.unlink(link)
        self.assertEqual(self.status(link["url"]), 404, "unlinked: gone at once")
        user.files.delete(shut)
        self.files.remove(shut["xid"])
        self.assertEqual(self.status(admins["url"]), 404, "file deleted: its links die")
        admin.files.folder(top, public=True, can_link=None)

        # 6. admin moves the subfolder to ROOT and deletes it
        self.assertEqual(admin.files.move(f"{top}sub2/", f"/sub2-py-{run}/")["path"], f"/sub2-py-{run}/")
        admin.files.delete(f"/sub2-py-{run}/")

        # 7. cleanup: nothing left behind
        for xid in list(self.files):
            _quiet(user.files.delete, xid)
            self.assertIsNone(user.files.info(xid))
            self.files.remove(xid)
        admin.files.delete(top)
        self.assertEqual(admin.files.list("/", q=f"F-py-{run}")["folders"], [])
        self.folders.clear()

    def test_async_client(self):
        cid, secret = _creds("USER")
        payload = bytes(range(256))

        async def run():
            async with AsyncClient(ENDPOINT, client_id=cid, client_secret=secret, timeout=30) as c:
                f = await c.files.put({"name": self.name("async.bin")}, payload)
                self.files.append(f["xid"])
                self.assertEqual(f["size"], 256)
                self.assertEqual(await c.files.download(f), payload)
                self.assertEqual(await c.files.download(f, byte_range=(250, None)), payload[250:])
                self.assertIsNone(await c.files.download(f, if_none_match=f["hash"]))
                with self.assertRaises(SynthigyError) as cm:
                    await c.files.put(f, b"x", if_match="0" * 64)
                self.assertEqual(cm.exception.code, "FILE_CHANGED")
                self.assertEqual(cm.exception.category, "conflict")
                await c.files.delete(f)
                self.assertIsNone(await c.files.info(f))
                self.files.remove(f["xid"])

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
