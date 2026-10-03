"""Synthigy codegen — .xsql operations + /schema → one typed Python module.

Usage (python3 -m synthigy.codegen <cmd>):

    gen <xsql...> [--schema P] [--out P]        describe + render Python
                  [--no-writes] [--pull]        (saves ops.ir.json)
                  [--endpoint URL]
    check <xsql...> [--endpoint URL]            CI drift gate (offline hash;
                                                live describe-diff when creds
                                                are present)

Environment: SYNTHIGY_ENDPOINT, and either SYNTHIGY_TOKEN or
SYNTHIGY_CLIENT_ID + SYNTHIGY_CLIENT_SECRET (neither = authless dev server).

Mirrors sdk/js/codegen/codegen.mjs. Two artifacts drive generation:

  * schema.json — nouns. GET /schema (IAM-filtered) → write-input
    types (`<Entity>Input`) and sync/stack/delete functions.
  * ops.ir.json — verbs. The server's op:"describe" compiles the .xsql
    (it owns the grammar AND the schema) and returns a language-neutral IR:
    typed result trees + params per operation. This emitter renders Python
    from that IR JSON and contains ZERO XSQL parsing.

The IR carries a sourceHash of the .xsql it was described from — a lockfile.
`check` fails offline when the sources drift from the saved IR ("edited
.xsql, forgot codegen"), and NEVER emits from an IR that disagrees with the
sources. Empty/unknown op kinds in the IR are a HARD error (a stale IR once
silently defaulted to search in the Go generator — never again).

Auth doctrine: codegen authenticates AS THE APP — the same client
credentials the app uses at runtime. /schema and describe are IAM-filtered
per principal, so the generated contract is exactly what the app can do.
"""

import argparse
import hashlib
import json
import keyword
import os
import py_compile
import re
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

from .facade import Client
from .errors import SynthigyError, error_from_server

# ── shared type mapping (the Python analog of sdk/js/codegen/lib.mjs) ────────

# scalar/wire type → Python. timestamps arrive as ISO strings on the wire.
PY_SCALAR = {
    "int": "int", "float": "float", "number": "float", "string": "str",
    "boolean": "bool", "timestamp": "str", "uuid": "str", "json": "Any",
    "transit": "Any", "encrypted": "str",
    # `?name:order` — a "column [asc|desc], …" spec string; the server
    # validates columns against the param's restriction set at bind time.
    "order": "str",
}

READ_VERBS = {"search", "get", "slice", "purge"}   # XSQL = read/destroy only
MUTATE_VERBS = {"sync", "stack", "delete"}          # schema-derived, never XSQL
KNOWN_VERBS = READ_VERBS | {"sql-template"}


def _words(s):
    out, cur = [], ""
    for ch in s:
        if ch in "-_ ":
            if cur:
                out.append(cur)
            cur = ""
        else:
            cur += ch
    if cur:
        out.append(cur)
    return out


def pascal(s):
    return "".join(w[:1].upper() + w[1:] for w in _words(s))


def entity_pascal(schema, n):
    """Server-rendered pascal skin
    for the entity schema[n], else the local guess."""
    ent = (schema or {}).get("entities", {}).get(n) or {}
    return (ent.get("skins") or {}).get("pascal") or pascal(n)


def safe_ident(s):
    """Valid Python identifier for a wire name — keywords get a trailing '_'."""
    return s + "_" if keyword.iskeyword(s) else s


class CodegenError(SystemExit):
    def __init__(self, message):
        print(f"codegen: {message}", file=sys.stderr)
        super().__init__(1)


# ── IR access ────────────────────────────────────────────────────────────────

def source_hash(source):
    return hashlib.sha256(source.encode()).hexdigest()


def _env_client(endpoint):
    """SDK client from env auth: SYNTHIGY_TOKEN | SYNTHIGY_CLIENT_ID+SECRET |
    authless (empty token — bare dev servers). Reuses the SDK's HTTP + OAuth
    stack; nothing is reimplemented here."""
    cid = os.environ.get("SYNTHIGY_CLIENT_ID")
    csec = os.environ.get("SYNTHIGY_CLIENT_SECRET")
    if cid and csec:
        return Client(endpoint, client_id=cid, client_secret=csec)
    return Client(endpoint, token=os.environ.get("SYNTHIGY_TOKEN") or "")


def describe(client, files):
    """POST op:"describe" — the server compiles each .xsql file on its own and returns the IR."""
    result = client.exec_([{"op": "describe", "sources": files}])[0]
    if not result.get("ok"):
        raise error_from_server(result.get("error"),
                                request_id=result.get("_request_id"))
    return result.get("data") or result.get("result") or {}


def pull_schema(endpoint, out_path):
    schema = _env_client(endpoint).schema()
    Path(out_path).write_text(json.dumps(schema, indent=2) + "\n")
    n = len(schema.get("entities") or {})
    v = schema.get("version")
    print(f"pulled {n} entities{' @' + str(v) if v else ''} → {out_path}")
    return schema


def _validate_ops(operations):
    """FAIL LOUD on empty/unknown op kinds (the stale-IR lesson); return
    (generatable_ops, batches) and warn-skip mutations."""
    ops, batches = [], []
    for o in operations:
        if o.get("batch"):
            batches.append(o)
            continue
        verb = o.get("op")
        if not verb:
            raise CodegenError(
                f"operation {o.get('name')!r} has an empty op kind — the IR "
                "is stale or corrupt; re-run: python3 -m synthigy.codegen gen")
        if verb in MUTATE_VERBS:
            print(f"skipped @{verb} {o.get('name')} — mutations are generated "
                  f"from the schema; use sync_{o.get('entity') or '<entity>'}"
                  "(data)", file=sys.stderr)
            continue
        if verb not in KNOWN_VERBS:
            raise CodegenError(
                f"operation {o.get('name')!r}: unknown op kind {verb!r} "
                f"(expected one of {sorted(KNOWN_VERBS)}) — the IR is stale "
                "or from a newer server; re-run gen / update the SDK")
        if verb == "sql-template" and not (o.get("namespace") or o.get("entity")):
            raise CodegenError(
                f"op {o.get('name')!r}: a sql-template with no root entity "
                "needs @namespace")
        ops.append(o)
    return ops, batches


def op_identity(o):
    """(namespace ?? entity, name) — bare names repeat across entities."""
    if o.get("batch"):
        return f"@batch {o.get('name')}"
    ns = (o.get("namespace") or o.get("entity") or "").lower()
    return f"{ns}/{str(o.get('name', '')).lower()}"


def resolve_member(ops, batch_name, ref):
    """Resolve a @batch member ref — bare `name` (must be unique) or
    qualified `ns/name`. Hard error on unknown/ambiguous."""
    ns, _, nm = ref.rpartition("/")
    ns, nm = (ns.lower() or None), nm.lower()
    matches = [o for o in ops
               if str(o.get("name", "")).lower() == nm
               and (ns is None
                    or (o.get("namespace") or o.get("entity") or "").lower() == ns)]
    if not matches:
        raise CodegenError(
            f"@batch {batch_name!r} references unknown/ungenerated op {ref!r}")
    if ns is None and len(matches) > 1:
        raise CodegenError(
            f"@batch {batch_name!r} — {ref!r} is ambiguous: "
            f"{', '.join(op_identity(m) for m in matches)} — qualify the reference")
    return matches[0]


# ── emitter ──────────────────────────────────────────────────────────────────

def _scalar_py(f):
    """IR result field → Python type expr. `nullable: false` ⇒ non-null."""
    if f.get("enum"):
        base = "Literal[" + ", ".join(repr(str(v)) for v in f["enum"]) + "]"
    else:
        base = PY_SCALAR.get(f.get("type"), "Any")
    return base if f.get("nullable") is False else f"{base} | None"


def _typeddict(name, entries):
    """Functional TypedDict syntax — safe for any wire key (keywords, etc.).
    `entries` = [(key, type-expr-string)]; pre-quoted exprs are forward refs."""
    lines = [f'{name} = TypedDict("{name}", {{']
    lines += [f'    "{k}": {t},' for k, t in entries]
    lines.append("})")
    return "\n".join(lines)


def _emit_rows(prefix, fields, chunks):
    """IR result tree → path-named Row TypedDicts (children first, so every
    reference is already defined). Relations are NotRequired — the wire OMITS
    empty relations, it never sends []."""
    entries = []
    for f in fields:
        k = f["key"]
        kind = f.get("kind")
        if kind == "relation":
            child = _emit_rows(prefix + pascal(k), f.get("fields") or [], chunks)
            inner = f"list[{child}]" if f.get("cardinality") == "many" else child
            entries.append((k, f"NotRequired[{inner}]"))
        elif kind == "map":   # _count / _agg — honest maps, alias-keyed wire
            v = PY_SCALAR.get(f.get("value"), "Any")
            entries.append((k, f"NotRequired[dict[str, {v}]]"))
        else:
            entries.append((k, _scalar_py(f)))
    chunks.append(_typeddict(prefix, entries))
    return prefix


def _emit_params(name, params, chunks):
    if not params:
        return None
    entries = []
    for p in params:
        t = PY_SCALAR.get(p.get("type"), "str")
        if p.get("array"):
            t = f"list[{t}]"
        if p.get("optional"):
            t = f"NotRequired[{t}]"
        entries.append((p["name"], t))
    chunks.append(_typeddict(name, entries))
    return name


def _src_const(ns, name):
    return f"_SRC_{ns.upper()}_{str(name).upper()}"


def _emit_source(const, source):
    lines = source.split("\n")
    if len(lines) == 1:
        return f"{const} = {lines[0]!r}"
    body = "\n".join(f"    {(line + chr(10))!r}" if i < len(lines) - 1
                     else f"    {line!r}"
                     for i, line in enumerate(lines))
    return f"{const} = (\n{body}\n)"


def _docstring(text, indent):
    doc = text.replace("\\", "\\\\").replace('"""', '\\"\\"\\"')
    if doc.endswith('"'):
        doc += " "
    pad = " " * indent
    if "\n" in doc:
        doc = doc.replace("\n", "\n" + pad)
        return f'{pad}"""{doc}\n{pad}"""'
    return f'{pad}"""{doc}"""'


CALL_OPTS = "*, acting_as: str | None = None, timeout: float | None = None"
CALL_PASS = "acting_as=acting_as, timeout=timeout"
WATCH_OPTS = "*, acting_as: str | None = None"
WATCH_PASS = "acting_as=acting_as"


def _params_sig(params_t, all_optional, known, label, opts_sig=CALL_OPTS):
    """(signature-fragment, params-pass-expression)."""
    if not params_t:
        return opts_sig, "{}"
    check = f"_params(params, {sorted(known)!r}, {label!r})"
    if all_optional:
        return f"params: {params_t} | None = None, {opts_sig}", check
    return f"params: {params_t}, {opts_sig}", check


def _emit_method(op, src, params_t, row_t):
    """One IR op → (sync_chunks, async_chunks): @staticmethod sources
    (class-body indented), each with its watch_<name> variant when the op
    declares @watch. The async twins run on the module-default AsyncClient
    (synthigy.aconnect) and return native-async watch handles."""
    name = safe_ident(str(op["name"]))
    verb = op["op"]
    is_sql = verb == "sql-template"
    all_opt = all(p.get("optional") for p in op.get("params") or [])
    known = [p["name"] for p in op.get("params") or []]
    sig, ppass = _params_sig(params_t, all_opt, known, name)
    wsig, _ = _params_sig(params_t, all_opt, known, name, WATCH_OPTS)
    ret = f"{row_t} | None" if verb == "get" else f"list[{row_t}]"

    lines = ["    @staticmethod", f"    def {name}({sig}) -> {ret}:"]
    if op.get("description"):
        lines.append(_docstring(op["description"], 8))
    if is_sql:
        lines.append(f"        return synthigy.sql_template({src}, {ppass}, {CALL_PASS})")
    else:
        lines.append(f"        return synthigy.query({src}, {ppass}, "
                     f"op={verb!r}, {CALL_PASS})")
    chunks = ["\n".join(lines)]

    alines = ["    @staticmethod", f"    async def {name}({sig}) -> {ret}:"]
    if op.get("description"):
        alines.append(_docstring(op["description"], 8))
    if is_sql:
        alines.append("        return await synthigy.aclient().sql_template("
                      f"{src}, {ppass}, {CALL_PASS})")
    else:
        alines.append("        return await synthigy.aclient().query("
                      f"{src}, {ppass}, op={verb!r}, {CALL_PASS})")
    achunks = ["\n".join(alines)]

    if op.get("watch"):
        wlines = ["    @staticmethod",
                  f"    def watch_{name}({wsig}) -> Any:"]
        wlines.append(_docstring(
            f"Live {op['name']} — notify-then-refetch watch over the same "
            "op. Returns the watch handle (.ready() / .events() / .close()).", 8))
        if is_sql:
            ents = op["watch"] if isinstance(op["watch"], list) else []
            wlines.append(f"        return synthigy.watch_sql_template({src}, "
                          f"{ppass}, entities={ents!r}, {WATCH_PASS})")
        else:
            wlines.append(f"        return synthigy.watch_query_xsql({src}, "
                          f"{ppass}, entity={op['entity']!r}, {WATCH_PASS})")
        chunks.append("\n".join(wlines))

        awlines = ["    @staticmethod",
                   f"    def watch_{name}({wsig}) -> Any:"]
        awlines.append(_docstring(
            f"Live {op['name']} — native-async watch (`async with ... as "
            "w`, `async for ev in w`; derived query/added|changed|removed "
            "events).", 8))
        if is_sql:
            ents = op["watch"] if isinstance(op["watch"], list) else []
            awlines.append("        return synthigy.aclient()"
                           f".watch_sql_template({src}, {ppass}, "
                           f"entities={ents!r}, {WATCH_PASS})")
        else:
            awlines.append("        return synthigy.aclient()"
                           f".watch_query_xsql({src}, {ppass}, "
                           f"entity={op['entity']!r}, {WATCH_PASS})")
        achunks.append("\n".join(awlines))
    return chunks, achunks


def _emit_batch(batch, ops, type_chunks):
    """@batch <name>: <m1> <m2> → ONE exec_ round trip; returns results keyed
    by member name (qualified ns/name when a bare name repeats). A failed
    member is a SynthigyError VALUE under its key — never a raised batch-wide
    error (mirrors sdk/clj gen.clj)."""
    name = safe_ident(str(batch["name"]))
    members = [resolve_member(ops, batch["name"], r) for r in batch["members"]]

    # merged params (dedupe by name; a param is optional only if optional in
    # every member that declares it)
    merged, order = {}, []
    for m in members:
        for p in m.get("params") or []:
            if p["name"] not in merged:
                merged[p["name"]] = dict(p)
                order.append(p["name"])
            elif not p.get("optional"):
                merged[p["name"]].pop("optional", None)
    params = [merged[n] for n in order]
    params_t = _emit_params(f"{pascal(name)}BatchParams", params, type_chunks)
    all_opt = all(p.get("optional") for p in params)
    sig, ppass = _params_sig(params_t, all_opt, order, name)

    dup = {n for n in {str(m["name"]).lower() for m in members}
           if sum(str(m["name"]).lower() == n for m in members) > 1}
    key = lambda m: (op_identity(m) if str(m["name"]).lower() in dup
                     else str(m["name"]))

    op_lines, res_lines = [], []
    for i, m in enumerate(members):
        ns = m.get("namespace") or m.get("entity")
        src = _src_const(ns, m["name"])
        if m["op"] == "sql-template":
            op_lines.append(f"        _ops.sql_template({src}, _p),")
        else:
            op_lines.append(f"        _ops.query({src}, _p, op={m['op']!r}),")
        res_lines.append(f'        "{key(m)}": _batch_result(_results[{i}], '
                         f"{m['op']!r}),")

    doc = _docstring(
        f"Batch: {' + '.join(batch['members'])} — ONE wire request; results "
        "keyed by member name. A failed member's value is a SynthigyError "
        "instance (not raised). Params are ONE flat dict shared by every "
        "member: a name two members declare binds the same value in both.", 4)
    body = ([f"    _p = {ppass if params_t else '{}'}", "    _operations = ["] + op_lines
            + ["    ]"])
    tail = ["    return {"] + res_lines + ["    }"]

    sync_fn = "\n".join(
        [f"def {name}({sig}) -> dict[str, Any]:", doc] + body
        + [f"    _results = synthigy.exec_(_operations, {CALL_PASS})"] + tail)
    async_fn = "\n".join(
        [f"async def {name}_async({sig}) -> dict[str, Any]:", doc] + body
        + [f"    _results = await synthigy.aclient().exec_(_operations, {CALL_PASS})"]
        + tail)
    return [sync_fn, async_fn]


_PARAMS_HELPER = '''\
def _params(params: Any, known: list[str], op: str) -> dict:
    """Reject params the operation doesn't declare — the server ignores them."""
    p = dict(params or {})
    unknown = sorted(set(p) - set(known))
    if unknown:
        raise SynthigyError(
            f"{op}: unknown param(s) {', '.join(unknown)}; "
            f"declared: {', '.join(known) or '(none)'}", "PARAM_UNKNOWN")
    return p'''


_BATCH_HELPER = '''\
def _batch_result(result: dict, op: str) -> Any:
    """Per-member unwrap: ok → data (get → record|None, else → list);
    error → a SynthigyError VALUE (the batch itself never raises)."""
    if not result.get("ok"):
        err = result.get("error") or {}
        return SynthigyError(err.get("message", "operation failed"),
                             err.get("code", "OPERATION_FAILED"),
                             err.get("details"))
    data = result.get("data")
    if op == "get":
        return data
    return data if data is not None else []'''


# ── write tier (schema-derived) ──────────────────────────────────────────────

def _referenced_entities(ops, schema):
    """Entities the operations touch: op roots, entity-named @namespaces, and
    every relation target reachable through the IR result trees (resolved via
    the schema — aliased relations that don't resolve are skipped)."""
    entities = schema.get("entities") or {}
    refs = set()

    def walk(entity, fields):
        if entity not in entities:
            return
        refs.add(entity)   # IR result trees are finite — no cycle guard needed
        rels = entities[entity].get("relations") or {}
        for f in fields or []:
            if f.get("kind") == "relation":
                rel = rels.get(f["key"])
                if rel and rel.get("to") in entities:
                    walk(rel["to"], f.get("fields"))

    for o in ops:
        ent = o.get("entity")
        if ent:
            walk(ent, (o.get("result") or {}).get("fields"))
        ns = o.get("namespace")
        if ns in entities:
            refs.add(ns)
    return sorted(refs)


def _attr_py_write(attr):
    """Schema attribute → Python INPUT type. hashed is write-only → plain str
    on input; reference types (type == an entity name) are FK id strings."""
    t = attr.get("type")
    if t == "enum":
        values = attr.get("enum") or []
        retired = set(attr.get("retired") or [])
        writable = [v for v in values if v not in retired]
        if writable:
            return "Literal[" + ", ".join(repr(str(v)) for v in writable) + "]"
        return "None" if values else "str"
    if t == "hashed":
        return "str"
    return PY_SCALAR.get(t, "str")


def _emit_write_types(entity, schema, referenced, chunks):
    ent = schema["entities"][entity]
    E = entity_pascal(schema, entity)
    rels = ent.get("relations") or {}
    in_entries = [("xid", "NotRequired[str]")]     # xid set = update target
    res_entries = [("xid", "str")]                 # writes always echo an xid
    for k, a in (ent.get("attributes") or {}).items():
        if k == "xid" or k in rels:
            continue
        t = _attr_py_write(a)
        read_t = _attr_py_write({**a, "retired": None})
        if a.get("nullable") is False:
            in_entries.append((k, t))              # required on input
        else:
            in_entries.append((k, f"NotRequired[{t}]" if t == "None" else f"NotRequired[{t} | None]"))
        res_entries.append((k, f"NotRequired[{read_t} | None]"))
    for k, r in rels.items():
        if r.get("to") not in (schema.get("entities") or {}):
            continue   # relation target filtered out of the schema → skip
        # a write relation is a LINK ({xid}) or a nested write; string
        # annotation = forward reference (entity graphs are cyclic)
        link = (f"_Link | {entity_pascal(schema, r['to'])}Input"
                if r["to"] in referenced else "dict[str, Any]")
        inner = f"list[{link}]" if r.get("cardinality") == "many" else link
        in_entries.append((k, f'"NotRequired[{inner}]"'))
        res_entries.append(
            (k, "NotRequired[list[dict[str, Any]]]"
             if r.get("cardinality") == "many" else "NotRequired[dict[str, Any]]"))
    chunks.append(_typeddict(f"{E}Input", in_entries))
    chunks.append(_typeddict(f"{E}WriteResult", res_entries))


def _emit_write_fns(entity, schema):
    E = entity_pascal(schema, entity)
    fn = safe_ident(entity)
    o = "acting_as: str | None = None, timeout: float | None = None"
    return f'''\
@overload
def sync_{fn}(data: {E}Input, *, returning: Literal[True],
              {o}) -> {E}WriteResult: ...
@overload
def sync_{fn}(data: list[{E}Input], *, returning: Literal[True],
              {o}) -> list[{E}WriteResult]: ...
@overload
def sync_{fn}(data: {E}Input | list[{E}Input], *, returning: Literal[False] = False,
              {o}) -> WriteCount: ...
def sync_{fn}(data, *, returning=False, acting_as=None, timeout=None):
    """Upsert {entity} record(s) — each relation key SENT is replaced exactly;
    keys left out are untouched.

    Silent by default: answers {{"count": n}}. Pass returning=True for the
    written records, or mint ids up front with synthigy.new_xid().
    """
    return synthigy.sync({entity!r}, data, returning=returning,
                         acting_as=acting_as, timeout=timeout)


@overload
def stack_{fn}(data: {E}Input, *, returning: Literal[True],
               {o}) -> {E}WriteResult: ...
@overload
def stack_{fn}(data: list[{E}Input], *, returning: Literal[True],
               {o}) -> list[{E}WriteResult]: ...
@overload
def stack_{fn}(data: {E}Input | list[{E}Input], *, returning: Literal[False] = False,
               {o}) -> WriteCount: ...
def stack_{fn}(data, *, returning=False, acting_as=None, timeout=None):
    """Additive write to {entity} — relation links are ADDED, never removed.

    Same returning contract as sync_{fn}.
    """
    return synthigy.stack({entity!r}, data, returning=returning,
                          acting_as=acting_as, timeout=timeout)


def delete_{fn}(xid: str, *, {o}) -> Any:
    """Delete one {entity} record by xid."""
    return synthigy.delete({entity!r}, {{"xid": xid}}, acting_as=acting_as,
                           timeout=timeout)'''


# ── render ───────────────────────────────────────────────────────────────────

def render(ir, schema=None, *, writes=True, input_name="<xsql>", regen=None):
    """IR (+ schema for the write tier) → generated Python module source."""
    if writes and schema is None:
        raise CodegenError("write tier needs a schema — pass --schema, keep a "
                           "schema.json beside the .xsql, or use --no-writes")
    ops, batches = _validate_ops(ir.get("operations") or [])

    # duplicate qualified identity = hard error (same rule the lint enforces)
    seen = {}
    for o in ops:
        oid = op_identity(o)
        if oid in seen:
            raise CodegenError(f"duplicate operation {oid!r} — "
                               "(namespace, name) must be unique")
        seen[oid] = o

    src_chunks, type_chunks, ns_map = [], [], {}
    for o in ops:
        ns = o.get("namespace") or o.get("entity")
        NS = entity_pascal(schema, ns)
        Type = f"{NS}{pascal(str(o['name']))}"
        src = _src_const(ns, o["name"])
        doc = (o["source"] if o["op"] == "sql-template"
               else f"@{o['op']} {o['name']}\n{o['source']}")
        src_chunks.append(_emit_source(src, doc))
        params_t = _emit_params(f"{Type}Params", o.get("params"), type_chunks)
        result = o.get("result") or {}
        if result.get("fields"):
            row_t = _emit_rows(f"{Type}Row", result["fields"], type_chunks)
        else:   # sql-template without @returns → untyped rows
            row_t = f"{Type}Row"
            type_chunks.append(f"{row_t} = dict[str, Any]")
        ns_map.setdefault(NS, {"ns": ns, "methods": [], "amethods": []})
        m, am = _emit_method(o, src, params_t, row_t)
        ns_map[NS]["methods"] += m
        ns_map[NS]["amethods"] += am

    class_chunks = []
    for NS in sorted(ns_map):
        g = ns_map[NS]
        body = "\n\n".join(g["methods"])
        class_chunks.append(
            f"class {NS}:\n"
            f'    """Typed operations for XSQL namespace \'{g["ns"]}\'."""\n\n'
            f"{body}")
        abody = "\n\n".join(g["amethods"])
        class_chunks.append(
            f"class {NS}Async:\n"
            f'    """Async twins for XSQL namespace \'{g["ns"]}\' — run on\n'
            f"    the module-default AsyncClient (synthigy.aconnect). Reads\n"
            f"    are awaitable; watch_* return native-async handles\n"
            f'    (`async with` / `async for`)."""\n\n'
            f"{abody}")

    batch_chunks = []
    for b in batches:
        batch_chunks += _emit_batch(b, ops, type_chunks)

    write_chunks = []
    if writes:
        referenced = _referenced_entities(ops, schema)
        if referenced:
            write_chunks.append(
                '# A write relation accepts a LINK to an existing record '
                '({"xid": ...})\n# or a full nested write.\n'
                '_Link = TypedDict("_Link", {"xid": str})\n\n'
                '# Writes are SILENT by default — the server answers {"count": n}.\n'
                '# Pass returning=True for the written records.\n'
                'WriteCount = TypedDict("WriteCount", {"count": int})')
        for e in referenced:
            _emit_write_types(e, schema, set(referenced), write_chunks)
        for e in referenced:
            write_chunks.append(_emit_write_fns(e, schema))

    imports = ["import synthigy"]
    if batch_chunks:
        imports.append("from synthigy import SynthigyError")
        imports.append("from synthigy import ops as _ops")
    schema_v = (schema or {}).get("version")
    header = (
        f"# AUTO-GENERATED by synthigy.codegen from {input_name} — DO NOT EDIT.\n"
        f"# sourceHash: {ir.get('sourceHash', '')}\n"
        + (f"# schema: /schema @{schema_v}\n" if writes and schema_v else "")
        + f"# Regenerate: {regen or f'python3 -m synthigy.codegen gen {input_name}'}\n"
        '"""Typed Synthigy operations — ONE async engine under both surfaces.\n'
        "Async apps: synthigy.aconnect(...) once at startup, then the *Async\n"
        "classes / *_async batch functions — awaitable reads, native-async\n"
        "watch handles (async with / async for). Async writes need no\n"
        "codegen: `await synthigy.aclient().stack(entity, data)` takes the\n"
        "same typed *Input dicts emitted below.\n\n"
        "Blocking scripts/notebooks: synthigy.connect(...) and the plain\n"
        "classes — same engine behind a one-thread blocking facade.\n\n"
        "Data keys are snake_case verbatim — Python native IS server native;\n"
        "no casing transform is applied anywhere.\n"
        '"""\n'
        "from __future__ import annotations\n\n"
        "from typing import Any, Literal, TypedDict, overload  # noqa: F401\n\n"
        "try:\n"
        "    from typing import NotRequired  # Python >= 3.11\n"
        "except ImportError:  # pragma: no cover\n"
        "    try:\n"
        "        from typing_extensions import NotRequired  # type: ignore\n"
        "    except ImportError:\n"
        "        class NotRequired:  # type: ignore  # runtime no-op; checkers use their own stubs\n"
        "            def __class_getitem__(cls, item):\n"
        "                return item\n\n"
        + "\n".join(imports) + "\n")

    parts = [header, _PARAMS_HELPER]
    if batch_chunks:
        parts.append(_BATCH_HELPER)
    parts += ["\n\n".join(src_chunks)] if src_chunks else []
    parts += type_chunks + class_chunks + batch_chunks + write_chunks
    return "\n\n\n".join(parts) + "\n"


# ── commands ─────────────────────────────────────────────────────────────────

def xsql_files(inputs):
    """Files as given; directories expanded recursively, sorted by relative path."""
    paths = []
    for a in inputs:
        p = Path(a)
        if p.is_dir():
            paths += sorted(p.rglob("*.xsql"), key=lambda f: f.relative_to(p).as_posix())
        elif p.exists():
            paths.append(p)
        else:
            raise CodegenError(f"{p} not found")
    if not paths:
        raise CodegenError(f"no .xsql files found in {' '.join(inputs)}")
    return paths


def xsql_dir(inputs):
    """Home of the IR and schema snapshots: the first input if a directory, else its parent."""
    p = Path(inputs[0])
    return p if p.is_dir() else p.parent


def _drop_workspace(src):
    return "\n".join(l for l in src.split("\n") if not re.match(r"@workspace\b", l))


def read_sources(paths):
    """Merge into one describe document; only the first file keeps its buffer-scoped `@workspace`."""
    return "\n\n".join(p.read_text() if i == 0 else _drop_workspace(p.read_text())
                        for i, p in enumerate(paths))


def read_files(paths, root):
    """`[{"path", "source"}]` for describe, paths relative to the xsql dir."""
    return [{"path": p.resolve().relative_to(root.resolve()).as_posix()
             if p.resolve().is_relative_to(root.resolve()) else p.name,
             "source": p.read_text()}
            for p in paths]


def _resolve_schema_path(dir_, override):
    """Locate the schema snapshot in the xsql dir, legacy name first so an existing file wins."""
    if override:
        return Path(override)
    for name in ("synthigy.schema.json", "schema.json"):
        p = dir_ / name
        if p.exists():
            return p
    return dir_ / "schema.json"   # auto-pull target


def _ir_path(dir_):
    return dir_ / "ops.ir.json"


def _instant(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return s


def warn_stale_schema(endpoint, schema_path):
    """One stderr line when schema_path's deploy stamp is older than the model at
    /.well-known/synthigy. Silent on any failure: generation stays offline-capable."""
    if not endpoint or not Path(schema_path).exists():
        return
    try:
        local = json.loads(Path(schema_path).read_text()).get("datasets") or {}
        url = endpoint.rstrip("/") + "/.well-known/synthigy"
        with urllib.request.urlopen(url, timeout=2) as resp:
            remote = json.load(resp).get("datasets") or {}
    except Exception:
        return
    changed = [i for i, at in remote.items()
               if i not in local or _instant(local[i].get("deployed-at")) != _instant(at)]
    changed += [i for i in local if i not in remote]
    if changed:
        print(f"warning: {schema_path} is stale against {endpoint} ({len(changed)} datasets "
              "changed since the pull) — re-run with --pull", file=sys.stderr)


def _load_ir(ir_path, source, files, *, endpoint, force_pull=False, offline_ok=True):
    """Cached IR when it matches the sources on disk; otherwise a live
    describe. NEVER returns an IR that disagrees with the sources."""
    sh = source_hash(source)
    cached = json.loads(ir_path.read_text()) if ir_path.exists() else None
    if cached and cached.get("sourceHash") == sh and not force_pull and offline_ok:
        print(f"using cached IR ({len(cached.get('operations') or [])} ops, "
              "in sync with sources) — use --pull to refresh")
        return cached, False
    if cached and cached.get("sourceHash") != sh:
        print("sources changed since IR pull — refreshing" if cached.get("sourceHash")
              else "saved IR predates source hashing — refreshing")
    try:
        fresh = describe(_env_client(endpoint), files)
    except SynthigyError as e:
        if e.category == "validation":
            raise CodegenError(f"describe failed: {e.code}: {e.message}")
        hint = ("run under `synthigy exec` or revert the .xsql edit"
                if cached else "run under `synthigy exec` (endpoint + credentials)")
        raise CodegenError(f"describe failed: {e.code}: {e.message} — {hint}")
    ir = {"sourceHash": sh, **fresh}
    return ir, True


def _default_out(inputs, dir_):
    p = Path(inputs[0])
    stem = p.stem if len(inputs) == 1 and not p.is_dir() else "ops"
    return dir_ / f"{stem}_gen.py"


def cmd_gen(args):
    paths = xsql_files(args.inputs)
    dir_ = xsql_dir(args.inputs)
    source = read_sources(paths)
    ir_path = _ir_path(dir_)
    ir, fresh = _load_ir(ir_path, source, read_files(paths, dir_), endpoint=args.endpoint,
                         force_pull=args.pull)
    if fresh:
        ir_path.write_text(json.dumps(ir, indent=2) + "\n")

    schema = None
    schema_path = _resolve_schema_path(dir_, args.schema)
    pulled = False
    if args.writes:
        if args.pull or not schema_path.exists():
            schema = pull_schema(args.endpoint, schema_path)
            pulled = True
        else:
            schema = json.loads(schema_path.read_text())
    if not pulled:
        warn_stale_schema(args.endpoint, schema_path)

    out = Path(args.out) if args.out else _default_out(args.inputs, dir_)
    if out.is_dir():
        out = out / _default_out(args.inputs, dir_).name
    # The output directory is normally gitignored, so on a fresh clone it does
    # not exist yet and write_text would raise FileNotFoundError.
    out.parent.mkdir(parents=True, exist_ok=True)
    regen = " ".join(["python3 -m synthigy.codegen gen", *args.inputs,
                      *(["--out", args.out] if args.out else []),
                      *([] if args.writes else ["--no-writes"])])
    code = render(ir, schema, writes=args.writes, input_name=" ".join(args.inputs),
                  regen=regen)
    out.write_text(code)

    # Gate, not a builder: the generated module must at least compile —
    # never ship broken output silently (the JS emitter gates on tsc).
    try:
        py_compile.compile(str(out), doraise=True)
    except py_compile.PyCompileError as e:
        raise CodegenError(f"generated module failed to compile (not shipped):\n{e}")

    n_ops = len([o for o in ir.get("operations") or [] if not o.get("batch")])
    n_batches = len([o for o in ir.get("operations") or [] if o.get("batch")])
    print(f"{len(paths)} .xsql file(s) → {n_ops} ops"
          + (f", {n_batches} batches" if n_batches else "")
          + ("" if args.writes else " [no-writes]")
          + f" → {out}")
    return 0


def cmd_check(args):
    paths = xsql_files(args.inputs)
    source = read_sources(paths)
    ir_path = _ir_path(xsql_dir(args.inputs))
    regen = f"python3 -m synthigy.codegen gen {' '.join(args.inputs)}"
    if not ir_path.exists():
        print(f"no saved IR at {ir_path} — run: {regen}", file=sys.stderr)
        return 1
    saved = json.loads(ir_path.read_text())
    sh = source_hash(source)
    # Offline, fast: catches "edited .xsql, forgot codegen" with no backend.
    if saved.get("sourceHash") != sh:
        print(f"sources drifted from saved IR — run: {regen}", file=sys.stderr)
        return 1
    # A hash match can still hide a server-side model change — live-diff when
    # credentials/endpoint are configured.
    if not any(os.environ.get(k) for k in
               ("SYNTHIGY_ENDPOINT", "SYNTHIGY_TOKEN", "SYNTHIGY_CLIENT_ID")):
        print("ok — sourceHash matches saved IR (offline; set SYNTHIGY_* env "
              "for a live describe-diff)")
        return 0
    fresh = {"sourceHash": sh,
             **describe(_env_client(args.endpoint), read_files(paths, xsql_dir(args.inputs)))}
    if json.dumps(saved, sort_keys=True) == json.dumps(fresh, sort_keys=True):
        print(f"ok — IR unchanged ({len(fresh.get('operations') or [])} ops)")
        return 0
    old = {op_identity(o): o for o in saved.get("operations") or []}
    new = {op_identity(o): o for o in fresh.get("operations") or []}
    added = sorted(set(new) - set(old))
    removed = sorted(set(old) - set(new))
    changed = sorted(k for k in set(old) & set(new)
                     if json.dumps(old[k], sort_keys=True)
                     != json.dumps(new[k], sort_keys=True))
    if added:
        print(f"+ added:   {', '.join(added)}")
    if removed:
        print(f"- removed: {', '.join(removed)}")
    if changed:
        print(f"~ changed: {', '.join(changed)}")
    print(f"IR drifted — run: {regen}", file=sys.stderr)
    return 1


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog=("python3 -m synthigy.codegen"
              if os.path.basename(sys.argv[0]) == "codegen.py" else None),
        description="Generate typed Python from XSQL operations + /schema.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    default_ep = os.environ.get("SYNTHIGY_ENDPOINT")

    g = sub.add_parser("gen", help="describe + generate a typed Python module")
    g.add_argument("inputs", nargs="+", metavar="xsql",
                   help=".xsql files and/or directories (searched recursively)")
    g.add_argument("--schema", help="explicit schema.json path")
    g.add_argument("--out", help="output .py path (default <file>_gen.py)")
    g.add_argument("--no-writes", dest="writes", action="store_false",
                   help="omit sync/stack/delete + Input types")
    g.add_argument("--pull", action="store_true",
                   help="force IR + schema refresh from the backend")
    g.add_argument("--endpoint", default=default_ep)

    c = sub.add_parser("check", help="CI drift gate (offline hash + live diff)")
    c.add_argument("inputs", nargs="+", metavar="xsql",
                   help=".xsql files and/or directories (searched recursively)")
    c.add_argument("--endpoint", default=default_ep)

    args = parser.parse_args(argv)
    if args.cmd == "gen":
        return cmd_gen(args)
    return cmd_check(args)


if __name__ == "__main__":
    sys.exit(main())
