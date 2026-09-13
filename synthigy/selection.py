"""Selection normalization — the load-bearing wire transform.

Shorthands (mixable at any nesting level):
    None / True            -> None (include scalar)
    ["name", "email"]      -> {"name": None, "email": None}
    {"roles": {"name": None}}
                           -> {"roles": [{"selections": {"name": None}}]}
    {"roles": [rel(...), rel(...)]}  -> passthrough, nested normalized

Join semantics are the SERVER's: absent "_join" is LEFT (a selection is a
projection and never drops parents; relation args filter the related rows).
The client injects nothing — pass {"_join": "inner"} explicitly when the
relation's existence should scope its parent.
"""


def rel(selections, args=None, alias=None):
    """Relation config for args / alias / repeated relations."""
    config = {"selections": normalize_selection(selections)}
    if args:
        config["args"] = args
    if alias:
        config["alias"] = alias
    return config


def fields(*names):
    """fields("a", "b") -> {"a": None, "b": None}"""
    return {n: None for n in names}


def _normalize_config(config):
    out = dict(config)
    if "selections" in config:
        out["selections"] = normalize_selection(config["selections"])
    return out


def normalize_selection(selection):
    if selection is None:
        return None
    if selection is True:
        return None

    if isinstance(selection, (list, tuple)):
        selection = list(selection)
        if selection and isinstance(selection[0], str):
            return {field: None for field in selection}
        return [_normalize_config(c) for c in selection]

    if not isinstance(selection, dict):
        return selection

    normalized = {}
    for key, value in selection.items():
        if value is None or value is True:
            normalized[key] = None
        elif isinstance(value, (list, tuple)):
            value = list(value)
            if value and isinstance(value[0], str):
                normalized[key] = [{"selections": normalize_selection(value)}]
            else:
                normalized[key] = [_normalize_config(c) for c in value]
        elif isinstance(value, dict):
            if "selections" in value:
                normalized[key] = [_normalize_config(value)]
            else:
                normalized[key] = [{"selections": normalize_selection(value)}]
        else:
            normalized[key] = value
    return normalized
