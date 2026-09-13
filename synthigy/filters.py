"""Where-clause operators — plain dict builders (data-shaped, no DSL).

Python keywords force renames vs the JS SDK: in_/nin, and_/or_/not_.
Note the wire mapping: gte -> _ge, lte -> _le.
"""


def _flat(values):
    # one-level flatten, mirroring JS `v.flat()`
    out = []
    for v in values:
        if isinstance(v, (list, tuple, set)):
            out.extend(v)
        else:
            out.append(v)
    return out


def eq(v): return {"_eq": v}
def neq(v): return {"_neq": v}
def gt(v): return {"_gt": v}
def gte(v): return {"_ge": v}
def lt(v): return {"_lt": v}
def lte(v): return {"_le": v}
def in_(*v): return {"_in": _flat(v)}
def nin(*v): return {"_nin": _flat(v)}
def like(v): return {"_like": v}
def ilike(v): return {"_ilike": v}
def is_null(): return {"_is_null": True}
def is_not_null(): return {"_is_not_null": True}


def and_(*clauses): return {"_and": _flat(clauses)}
def or_(*clauses): return {"_or": _flat(clauses)}
def not_(clause): return {"_not": clause}
