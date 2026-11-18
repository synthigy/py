"""Watch backfill: fold /history event rows into record events."""


def fold_history_events(events, records, since=None):
    """Fold attribute-level /history rows into one record event per (record, op) after `since`, marked fromBackfill."""
    by_key = {}
    for ev in events or []:
        xid = ev.get("recordXid") or ev.get("record-xid")
        ts = ev.get("ts")
        if not xid or xid not in records or not ts or (since and not ts > since):
            continue
        op = ev.get("op")
        channel_type = f"record/{'update' if op == 'change' else op}"
        folded = by_key.get((xid, channel_type))
        if folded is None:
            folded = {
                "type": channel_type,
                "record": xid,
                "ts": ts,
                "txid": ev.get("txid"),
                "actor": ev.get("actorXid") or ev.get("actor-xid"),
                "request": ev.get("requestId") or ev.get("request-id"),
                "scope": ev.get("scopeXid") or ev.get("scope-xid"),
                "after": {},
                "fromBackfill": True,
            }
            by_key[(xid, channel_type)] = folded
        attr_xid = ev.get("attributeXid") or ev.get("attribute-xid")
        if attr_xid and attr_xid != "__delete__":
            folded["after"][attr_xid] = ev.get("value")
        if ts > folded["ts"]:
            folded["ts"] = ts
    out = sorted(by_key.values(), key=lambda e: e["ts"])
    for folded in out:
        if folded["type"] == "record/delete":
            folded.pop("after", None)
    return out
