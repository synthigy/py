"""Pure event/interest/diff helpers shared by the client and watch layer.

Everything here is side-effect-free data-shaping: raw channel envelopes →
user-facing events, interest normalization + matching, result-set diffing,
subscription descriptors. Ports of the equivalent pure layers in
sdk/js watch.js and sdk/go watch.go — before/after keys pass through
VERBATIM (server-native snake_case); `changed` is computed client-side.
"""

import json
import re

from .errors import SynthigyError

# seconds; watch trigger events within a window fold to one refetch
REFRESH_COALESCE = 0.05


# ---------------------------------------------------------------------------
# Event shaping + matching
# ---------------------------------------------------------------------------


def _shallow_eq(a, b):
    if a is b:
        return True
    try:
        return json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
    except (TypeError, ValueError):
        return a == b


def compute_changed(before, after, op):
    """insert -> after keys, delete -> before keys, update -> shallow diff
    over after's keys. Sorted. None when uncomputable."""
    if op == "insert":
        return sorted(after) if after else None
    if op == "delete":
        return sorted(before) if before else None
    if not before or not after:
        return None
    return sorted(k for k in after if not _shallow_eq(before.get(k), after[k]))


def shape_event(env):
    """Translate a raw channel envelope into the user-facing event shape.
    before/after keys stay verbatim snake_case; changed is computed."""
    t = env.get("type")
    if not isinstance(t, str) or "/" not in t:
        return None
    track, _, op = t.partition("/")
    # Coalesced cache-invalidation pokes — names echo back verbatim.
    if t == "entity/touched":
        return {"type": t, "entity": env.get("entity"), "ts": env.get("ts")}
    if t == "relation/touched":
        return {"type": t, "relation": env.get("relation"), "ts": env.get("ts")}
    base = {
        "type": t,
        "ts": env.get("ts"),
        "txid": env.get("txid"),
        "actor": env.get("actor"),
        "request": env.get("request"),
        "tenant": env.get("tenant"),
        "scope": env.get("scope"),
    }
    if track == "record":
        before, after = env.get("before"), env.get("after")
        out = dict(base, record=env.get("record-xid"))
        if before is not None:
            out["before"] = before
        if after is not None:
            out["after"] = after
        changed = compute_changed(before, after, op)
        if changed is not None:
            out["changed"] = changed
        return out
    if track == "relation":
        return dict(base, data=env.get("data"))
    return None


def _op_match(ops, event_type):
    if not ops:
        return True
    return event_type.partition("/")[2] in ops


def matches(interest, env):
    """True when a raw envelope belongs to this interest. record/* by
    record-xid; relation/link|unlink by data[0] (server rotates so the
    subscribed record sits at position 0); touch pokes by literal name.
    relation_xids never match here — they only keep an interest non-empty
    (same as JS/Go matchers)."""
    t = env.get("type")
    records = interest.get("records")
    if t in ("record/insert", "record/update", "record/delete"):
        if records and env.get("record-xid") in records:
            return _op_match(interest.get("ops"), t)
        return False
    if t in ("relation/link", "relation/unlink"):
        data = env.get("data")
        if (records and isinstance(data, (list, tuple)) and len(data) >= 2
                and data[0] in records):
            return _op_match(interest.get("ops"), t)
        return False
    if t == "entity/touched":
        entities = interest.get("entities")
        return bool(entities) and env.get("entity") in entities
    if t == "relation/touched":
        relations = interest.get("relations")
        return bool(relations) and env.get("relation") in relations
    return False


def _dedupe_list(vals, name):
    if vals is None:
        return None
    if not isinstance(vals, (list, tuple)):
        raise SynthigyError(f"interest.{name} must be a list",
                            "INVALID_INTEREST")
    out, seen = [], set()
    for v in vals:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def normalize_interest(interest):
    """{records?, entities?, relations?, relation_xids?, ops?} — dedupe,
    validate list-ness; at least one of records/entities/relations/
    relation_xids required. relation_xids are a LOCAL matcher aid only,
    never on the wire."""
    interest = interest or {}
    out = {}
    for key in ("records", "relations", "relation_xids", "ops", "entities"):
        vals = _dedupe_list(interest.get(key), key)
        if vals is not None:
            out[key] = vals
    if not (out.get("records") or out.get("relations")
            or out.get("relation_xids") or out.get("entities")):
        raise SynthigyError(
            "watch() requires at least one of: records, entities, "
            "relations, relation_xids", "EMPTY_INTEREST")
    return out


# ---------------------------------------------------------------------------
# Result-set diffing (watch_query)
# ---------------------------------------------------------------------------


def _row_key(r):
    if not isinstance(r, dict):
        return None
    return r.get("xid") or r.get("euuid")


def _collect_all_xids(rows):
    """Every xid in the result tree — top-level + nested — so updates on
    nested records reach the parent query."""
    out = set()

    def visit(v):
        if isinstance(v, dict):
            xid = v.get("xid")
            if isinstance(xid, str):
                out.add(xid)
            for k, vv in v.items():
                if k != "xid":
                    visit(vv)
        elif isinstance(v, (list, tuple)):
            for e in v:
                visit(e)

    for r in rows or []:
        visit(r)
    return out


def _json_eq(a, b):
    try:
        return json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
    except (TypeError, ValueError):
        return a == b


def _rows_equal(a, b):
    if a is b:
        return True
    if not isinstance(a, dict) or not isinstance(b, dict):
        return False
    if set(a) != set(b):
        return False
    return all(_json_eq(a[k], b[k]) for k in a)


def _diff_row(before, after):
    before = before or {}
    after = after or {}
    changed, bmap, amap = [], {}, {}
    for k in sorted(set(before) | set(after)):
        if not _json_eq(before.get(k), after.get(k)):
            changed.append(k)
            bmap[k] = before.get(k)
            amap[k] = after.get(k)
    return changed, bmap, amap


def _union_changed(a, b):
    if not a:
        return b
    if not b:
        return a
    out = list(a)
    seen = set(a)
    for x in b:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


# ---------------------------------------------------------------------------
# Subscription descriptors (observe / raw subscription set)
# ---------------------------------------------------------------------------


def normalize_descriptor(arg):
    """['xid', ...] or {"records": [...], "operations": [...]} ->
    {"records": set, "operations": set?}. Rejects the legacy
    entity-firehose string form."""
    if isinstance(arg, str):
        raise SynthigyError(
            "Subscriptions are records-only. Pass a list of xids or "
            "{'records': [...]} instead of an entity name.",
            "INVALID_SUBSCRIPTION")
    if isinstance(arg, (list, tuple, set)):
        if not arg:
            raise SynthigyError("records must be a non-empty list of xid strings",
                                "EMPTY_RECORDS")
        return {"records": set(arg)}
    if isinstance(arg, dict) and isinstance(arg.get("records"), (list, tuple, set)):
        if not arg["records"]:
            raise SynthigyError("records must be a non-empty list of xid strings",
                                "EMPTY_RECORDS")
        out = {"records": set(arg["records"])}
        if "operations" in arg and arg["operations"] is not None:
            if not isinstance(arg["operations"], (list, tuple, set)):
                raise SynthigyError("operations must be a list of vocab strings",
                                    "INVALID_OPERATIONS")
            out["operations"] = set(arg["operations"])
        return out
    raise SynthigyError(
        "Expected a list of xids or a {records, operations?} descriptor",
        "INVALID_SUBSCRIPTION")


def descriptor_key(descriptor):
    """Stable hash key: sorted records + sorted operations — re-subscribing
    with the same descriptor is idempotent."""
    return json.dumps({
        "records": sorted(descriptor["records"]),
        "operations": sorted(descriptor["operations"])
        if "operations" in descriptor else None,
    })


def _event_matches_descriptor(event, descriptor):
    """Record events match by record-xid; relation events by data[0]
    (server rotates so position 0 is the subscribed perspective)."""
    records = descriptor.get("records")
    if not records:
        return False
    record_xid = event.get("record-xid")
    if record_xid:
        return record_xid in records
    data = event.get("data")
    if isinstance(data, list) and len(data) >= 2:
        return data[0] in records
    return False


# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------


def kebab(s):
    """camelCase / snake_case / spaced -> kebab-case (mirrors JS kebab())."""
    if s is None:
        return None
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1-\2", str(s))
    return re.sub(r"[\s_]+", "-", s).lower()
