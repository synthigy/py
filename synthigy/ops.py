"""Raw wire-operation builders for exec_() batching.

Usage:
    from synthigy import ops
    results = synthigy.exec_([ops.slice("User", {"xid": u}, {"roles": [{"xid": r}]}),
                              ops.stack("User", {"xid": u, "roles": [{"xid": r2}]})])
"""

from .selection import normalize_selection
from .util import xsql_document


def search(entity, args=None, selection=None):
    return {"op": "search", "entity": entity, "args": args,
            "selections": normalize_selection(selection)}


def get(entity, args=None, selection=None):
    return {"op": "get", "entity": entity, "args": args,
            "selections": normalize_selection(selection)}


def sync(entity, data, returning=False):
    """The server answers {"count": n}; pass returning=True for the written
    records. Mint ids with new_xid() when you need them up front."""
    return {"op": "sync", "entity": entity, "data": data,
            "returning": bool(returning)}


def stack(entity, data, returning=False):
    """Same returning contract as sync."""
    return {"op": "stack", "entity": entity, "data": data,
            "returning": bool(returning)}


def slice(entity, args, selection=None):  # noqa: A001 — mirrors the wire op name
    return {"op": "slice", "entity": entity, "args": args,
            "selections": normalize_selection(selection)}


def delete(entity, data):
    return {"op": "delete", "entity": entity, "data": data}


def purge(entity, args, selection=None):
    return {"op": "purge", "entity": entity, "args": args,
            "selections": normalize_selection(selection)}


def search_tree(entity, on, args=None, selection=None):
    return {"op": "search-tree", "entity": entity, "on": on, "args": args,
            "selections": normalize_selection(selection)}


def get_tree(entity, root, on, selection=None):
    return {"op": "get-tree", "entity": entity, "root": root, "on": on,
            "selections": normalize_selection(selection)}


def sql_template(template, params=None):
    return {"op": "sql-template", "template": template, "params": params or []}


def query(xsql, params=None, op="search"):
    # STRICT wire: XSQL travels only as the `xsql` document op.
    o = {"op": "xsql", "xsql": xsql_document(xsql, op)}
    if params is not None:
        o["params"] = params
    return o


def deploy(export_contents):
    return {"op": "deploy", "data": export_contents}


def destroy(dataset_xid):
    return {"op": "delete", "entity": "dataset", "data": {"xid": dataset_xid}}


def deployed_model():
    return {"op": "deployed-model"}


def runtime_model():
    return {"op": "runtime-model"}
