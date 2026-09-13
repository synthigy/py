"""Tree composition — nest flat search-tree / get-tree results.

Pure functions, no client. Two-pass id-level indexing (record-by-id +
parent->children-ids) so copied children never hold stale subtrees.
Cycle-safe: a record participating in a cycle is dropped from that branch.
"""


def _record_id(record):
    if not isinstance(record, dict):
        return None
    for k in ("xid", "euuid", "_eid"):
        if record.get(k) is not None:
            return record[k]
    return None


def _parent_id(record, on):
    if not record:
        return None
    # tolerate kebab/snake/camel casings of the relation key
    for k in (on, on.replace("-", "_"), on.replace("_", "-")):
        if k in record:
            v = record[k]
            if v is None:
                return None
            if isinstance(v, dict):
                return _record_id(v)
            return v
    return None


def _build_indexes(records, on):
    by_id, kids = {}, {}
    for r in records:
        rid = _record_id(r)
        if rid is None:
            continue
        by_id[rid] = r
        pid = _parent_id(r, on)
        if pid is not None and pid != rid:
            kids.setdefault(pid, []).append(rid)
    return by_id, kids


def _build_subtree(by_id, kids, children_key, rid, visited):
    if rid in visited:
        return None  # cycle — break
    visited.add(rid)
    children = [
        sub for cid in kids.get(rid, [])
        if (sub := _build_subtree(by_id, kids, children_key, cid, visited)) is not None
    ]
    visited.discard(rid)
    return {**by_id[rid], children_key: children}


def compose_tree(records, on, root_id=None, children_key="_children"):
    """Compose a flat list into one tree rooted at root_id (default: first
    record). Returns None when the root isn't in the set."""
    if not records:
        return None
    if not on:
        raise ValueError("compose_tree: `on` (relation name) is required")
    by_id, kids = _build_indexes(records, on)
    root = root_id if root_id is not None else _record_id(records[0])
    if root not in by_id:
        return None
    return _build_subtree(by_id, kids, children_key, root, set())


def compose_forest(records, on, children_key="_children"):
    """Compose a flat list into a forest — one tree per record whose parent
    isn't present in the set."""
    if not records:
        return []
    if not on:
        raise ValueError("compose_forest: `on` (relation name) is required")
    by_id, kids = _build_indexes(records, on)
    roots = []
    for r in records:
        rid = _record_id(r)
        if rid is None:
            continue
        pid = _parent_id(r, on)
        if pid is None or pid not in by_id or pid == rid:
            roots.append(rid)
    return [_build_subtree(by_id, kids, children_key, rid, set()) for rid in roots]
