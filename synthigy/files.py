"""Files: the bytes behind /files, each also a File record on /data."""

from __future__ import annotations

import json
import re
import urllib.parse

from .errors import SynthigyError, error_from_server
from .util import DEFAULT_TIMEOUT

_ANY = object()

SERVED_HEADERS = ("content-type", "content-length", "content-range", "accept-ranges", "etag",
                  "content-disposition", "cache-control", "x-content-type-options", "content-security-policy")


def _xid(x):
    return x.get("xid") if isinstance(x, dict) else x


def _q(xid):
    return urllib.parse.quote(str(xid), safe="")


def _is_path(x):
    return isinstance(x, str) and x.startswith("/")


def _is_folder(x):
    if isinstance(x, dict):
        x = x.get("path")
    return _is_path(x) and x.endswith("/")


def _file_ref(x):
    return (x.get("xid") or x.get("path")) if isinstance(x, dict) else x


def _folder_ref(x):
    return (x.get("path") or x.get("xid")) if isinstance(x, dict) else x


def _file_at(ref, tail=""):
    """-> (path, query) for one file: /<xid>, or ?path= for a path."""
    if _is_path(ref):
        return tail, {"path": ref}
    return f"/{_q(_xid(ref))}{tail}", None


def _folder_at(ref, tail=""):
    """-> (path, query) for one folder: /folders/<xid>, or /folders?path= for a path."""
    ref = _folder_ref(ref)
    if _is_path(ref):
        return "/folders" + tail, {"path": ref}
    return f"/folders/{_q(ref)}{tail}", None


def _body(data, content_type):
    """-> (bytes, content-type) for a put."""
    if hasattr(data, "read"):
        data = data.read()
    if isinstance(data, str):
        return data.encode("utf-8"), content_type or "text/plain; charset=utf-8"
    if isinstance(data, (bytes, bytearray, memoryview)):
        return bytes(data), content_type or "application/octet-stream"
    raise SynthigyError("put takes bytes, str or a binary file", "INVALID_BODY")


def _grantees(users, groups):
    return {"users": [_xid(u) for u in users], "groups": [_xid(g) for g in groups]}


class AsyncFilesAPI:
    """Store, read and share files; a file is a path (/a/b/report.pdf), an xid or the file itself,
    a folder a path ending in / (/ is ROOT), an xid or the folder itself."""

    def __init__(self, client):
        self._client = client

    def _query(self, query):
        kf = self._client._default_key_format
        query = {**(query or {}), **({"key_format": kf} if kf and kf != "snake" else {})}
        qs = urllib.parse.urlencode({k: v for k, v in query.items() if v is not None})
        return f"?{qs}" if qs else ""

    async def _send(self, method, path, *, query=None, body=None, headers=None, file_bytes=False):
        status, resp_headers, data = await self._client._authed(
            method, "/files" + path + self._query(query), body=body, headers=headers,
            timeout=None if file_bytes else DEFAULT_TIMEOUT)
        if 200 <= status < 300 or status == 304:
            return status, resp_headers, data
        request_id = resp_headers.get("x-request-id")
        try:
            parsed = json.loads(data) if data else None
        except ValueError:
            parsed = None
        err = parsed.get("error") if isinstance(parsed, dict) else None
        if (isinstance(err, dict) and err.get("code") == "NOT_FOUND") or (status == 404 and not err):
            raise SynthigyError("Files are not available on this server — is the files module running?",
                                "FILES_UNAVAILABLE", request_id=request_id, status=status)
        if isinstance(err, dict):
            raise error_from_server(err, status, request_id)
        raise SynthigyError(f"Files request failed ({status})", "HTTP_ERROR",
                            data.decode("utf-8", "replace") if data else None,
                            request_id=request_id, status=status)

    async def _json(self, method, path, payload=None, query=None):
        headers, body = {}, None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(payload).encode()
        _s, _h, data = await self._send(method, path, query=query, body=body, headers=headers)
        return json.loads(data) if data else None

    async def _xid_at(self, ref):
        """-> the xid of a file named by path or xid; versions take only an xid."""
        if not _is_path(ref):
            return _xid(ref)
        f = await self.info(ref)
        if not f:
            raise SynthigyError(f"No such file: {ref}", "FILE_NOT_FOUND", status=404)
        return f["xid"]

    async def put(self, target, data, *, content_type=None, if_match=None):
        """Store bytes, a str or a binary file. A path (/a/b/name, the folder must exist) creates
        the file or replaces it; an xid or the file replaces it; {"name": ...} makes a new file in no folder."""
        raw, ctype = _body(data, content_type)
        if ctype.lower().startswith("application/x-www-form-urlencoded"):
            raise SynthigyError("a form-encoded body is parsed as a form, not stored — "
                                "send the bytes with their own type", "UNSUPPORTED_MEDIA_TYPE")
        expected = if_match.get("hash") if isinstance(if_match, dict) else if_match
        method = "PUT"
        if _is_path(target):
            path, query = "", {"path": target}
        elif _xid(target):
            path, query = f"/{_q(_xid(target))}", None
        elif isinstance(target, dict) and target.get("name"):
            method, path, query = "POST", "", {"name": target["name"]}
        else:
            raise SynthigyError("put takes a path, an xid or file, or {'name': ...} for a file in no folder",
                                "INVALID_PATH")
        headers = {"Content-Type": ctype}
        if expected:
            headers["If-Match"] = f'"{expected}"'
        _s, _h, out = await self._send(method, path, query=query, body=raw, headers=headers, file_bytes=True)
        return json.loads(out)

    async def download(self, file, *, byte_range=None, if_none_match=None, version=None):
        """The bytes of a file, or None while `if_none_match` still matches; `byte_range=(start, end)`
        is inclusive, `version` reads a kept one."""
        headers = {}
        if byte_range is not None:
            start, end = (tuple(byte_range) + (None,))[:2]
            headers["Range"] = f"bytes={'' if start is None else start}-{'' if end is None else end}"
        if if_none_match:
            headers["If-None-Match"] = f'"{if_none_match}"'
        if version is None:
            path, query = _file_at(file)
        else:
            vid = version.get("id") if isinstance(version, dict) else version
            path, query = f"/{_q(await self._xid_at(file))}/versions/{_q(vid)}", None
        status, _h, data = await self._send("GET", path, query=query, headers=headers, file_bytes=True)
        return None if status == 304 else data

    async def zip(self, *, files=(), folders=()):
        """One zip (bytes) of `files` and of everything inside `folders`, read as you; what you
        cannot read is listed in its _skipped.txt."""
        payload = {"files": [_file_ref(f) for f in files], "folders": [_folder_ref(d) for d in folders]}
        _s, _h, data = await self._send("POST", "/zip", body=json.dumps(payload).encode(),
                                        headers={"Content-Type": "application/json"}, file_bytes=True)
        return data

    async def info(self, file):
        """The file, or None when it is not there or you cannot see it."""
        try:
            path, query = _file_at(file, "/info")
            return await self._json("GET", path, query=query)
        except SynthigyError as e:
            if e.code in ("FILE_NOT_FOUND", "FOLDER_NOT_FOUND"):
                return None
            raise

    async def list(self, folder=_ANY, *, q=None, limit=None, offset=None):
        """A folder's {"files", "folders"}; None: the files in no folder; omitted: every file you
        can see, newest first. `q` keeps names holding its words in order."""
        page = {"q": q or None, "limit": limit, "offset": offset}
        if folder is _ANY:
            return {"files": (await self._json("GET", "", query=page))["files"], "folders": []}
        if folder is None:
            return {"files": (await self._json("GET", "", query={**page, "folderless": "true"}))["files"],
                    "folders": []}
        ref = _folder_ref(folder)
        files = (await self._json("GET", "", query={**page, "folder": ref}))["files"]
        folders = (await self._json("GET", "/folders", query={"parent": ref}))["folders"]
        words = (q or "").split()
        if words:
            holds = re.compile(".*".join(re.escape(w) for w in words), re.IGNORECASE)
            folders = [f for f in folders if holds.search(f.get("name") or "")]
        return {"files": files, "folders": folders}

    async def search(self, q, *, folder=None, limit=None, offset=None):
        """Folders and files whose paths hold the words of `q` in order, shortest path first:
        {"results", "truncated"}; `folder` keeps it below that folder."""
        return await self._json("GET", "/search", query={
            "q": q, "folder": _folder_ref(folder) if folder is not None else None,
            "limit": limit, "offset": offset})

    async def delete(self, ref):
        """Delete a file, or an empty folder (a path ending in / or the folder itself)."""
        path, query = _folder_at(ref) if _is_folder(ref) else _file_at(ref)
        await self._send("DELETE", path, query=query)

    def url(self, file, *, inline=False):
        """Where a file loads from, without a request: /files/<xid>, or /files/path/... for a path;
        `inline` opens it in the browser instead of downloading it."""
        if _is_path(file):
            if file.endswith("/"):
                raise SynthigyError("url takes a file, not a folder", "INVALID_PATH")
            u = f"{self._client._endpoint}/files/path{urllib.parse.quote(file, safe='/')}"
        else:
            u = f"{self._client._endpoint}/files/{_q(_xid(file))}"
        return u + "?disposition=inline" if inline else u

    async def move(self, src, to):
        """Rename or move. A file goes to `to` (/a/new.png), or into a folder when `to` ends in /
        (the name stays); a folder's `to` is its new path."""
        if not _is_path(to):
            raise SynthigyError("move takes the new path, for example /site/brand/logo.png", "INVALID_PATH")
        if _is_folder(src):
            parts = [p for p in to.split("/") if p]
            if not parts:
                raise SynthigyError("a folder moves to a path like /archive/2026/", "INVALID_PATH")
            path, query = _folder_at(src)
            parent = "/" + "/".join(parts[:-1]) + "/" if len(parts) > 1 else "/"
            return await self._json("PATCH", path, {"name": parts[-1], "parent": parent}, query)
        cut = to.rfind("/")
        path, query = _file_at(src)
        payload = {"folder": to} if to.endswith("/") else {"folder": to[:cut + 1], "name": to[cut + 1:]}
        return await self._json("PATCH", path, payload, query)

    async def folder(self, path, *, public=_ANY, cache=_ANY, can_link=_ANY):
        """The folder at `path`, made with every missing folder above it; `public`, `cache` and
        `can_link` set its own settings (None: inherit from above)."""
        settings = {k: v for k, v in (("public", public), ("cache", cache), ("can_link", can_link))
                    if v is not _ANY}
        return await self._json("PUT", "/folders", settings, {"path": path})

    async def share(self, folder, access, *, users=(), groups=()):
        """Give users and groups (xids or records) `access` (read, write or delete) on a folder;
        writers of the folder may."""
        path, query = _folder_at(folder, "/share")
        await self._json("POST", path, {"access": access, **_grantees(users, groups)}, query)

    async def unshare(self, folder, access, *, users=(), groups=()):
        """Take `access` on a folder away from users and groups."""
        path, query = _folder_at(folder, "/unshare")
        await self._json("POST", path, {"access": access, **_grantees(users, groups)}, query)

    async def access(self, folder):
        """Who holds read, write and delete on a folder."""
        path, query = _folder_at(folder, "/access")
        return await self._json("GET", path, query=query)

    async def versions(self, file):
        """The kept versions (the last ten), newest first."""
        return (await self._json("GET", f"/{_q(await self._xid_at(file))}/versions"))["versions"]

    async def restore(self, file, version):
        """Make a kept version current again; no bytes are copied."""
        vid = version.get("id") if isinstance(version, dict) else version
        return await self._json("POST", f"/{_q(await self._xid_at(file))}/versions/{_q(vid)}/restore")

    async def link(self, file, *, expires=None, max_downloads=None, inline=None):
        """A link that downloads the file without signing in, where the folder's can_link allows it;
        `expires` seconds (default 3600, at most 7 days). Its `url` is shown only now."""
        opts = {"expires": expires, "max_downloads": max_downloads, "inline": inline}
        return await self._json("POST", f"/{_q(await self._xid_at(file))}/links",
                                {k: v for k, v in opts.items() if v is not None})

    async def links(self, file):
        """The file's live links you can see: your own, or all of them for File Admin."""
        return (await self._json("GET", f"/{_q(await self._xid_at(file))}/links"))["links"]

    async def unlink(self, link):
        """End a link (the link or its xid) at once, on every engine."""
        await self._send("DELETE", f"/links/{_q(_xid(link))}")

    async def serve(self, file, headers=None, *, method="GET", inline=False):
        """Answer a browser's request with the file: -> (status, headers, body). Range, If-None-Match
        and If-Range from `headers` go through; a refusal is the engine's own answer."""
        incoming = {k.lower(): v for k, v in (headers or {}).items()}
        h = {k: incoming[k.lower()] for k in ("Range", "If-None-Match", "If-Range") if incoming.get(k.lower())}
        path, query = _file_at(file)
        query = {**(query or {}), **({"disposition": "inline"} if inline else {})}
        status, resp_headers, data = await self._client._authed(
            "HEAD" if method == "HEAD" else "GET", "/files" + path + self._query(query), headers=h, timeout=None)
        out = {k: resp_headers[k] for k in SERVED_HEADERS if resp_headers.get(k)}
        return status, out, b"" if method == "HEAD" else data
