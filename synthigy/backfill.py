"""Observe-gap backfill — fold /history event rows into observe events.

History rows are attribute-primary (one row per changed attribute) and
key by attribute-XID; live events are attribute-NAME keyed. Backfill
events are left xid-keyed and flagged from_backfill=True so consumers
can tell them apart. Best-effort by contract.
"""


def fold_history_events(events, records):
    """events: /history 'events' rows; records: watched xid set.
    Returns folded observe-shaped events, one per (record, op)."""
    by_key = {}
    order = []
    for ev in events or []:
        xid = ev.get("recordXid") or ev.get("record-xid")
        if not xid or xid not in records:
            continue
        op = ev.get("op")
        fold_key = f"{xid}::{op}"
        channel_type = f"record/{'update' if op == 'change' else op}"
        folded = by_key.get(fold_key)
        if folded is None:
            folded = {
                "type": channel_type,
                "ts": ev.get("ts"),
                "record-xid": xid,
                "txid": ev.get("txid"),
                "actor": ev.get("actorXid") or ev.get("actor-xid"),
                "request": ev.get("requestId") or ev.get("request-id"),
                "scope": ev.get("scopeXid") or ev.get("scope-xid"),
                "after": {},
                "fromBackfill": True,
            }
            by_key[fold_key] = folded
            order.append(folded)
        attr_xid = ev.get("attributeXid") or ev.get("attribute-xid")
        if attr_xid and attr_xid != "__delete__":
            folded["after"][attr_xid] = ev.get("value")
        if (ev.get("ts") or "") > (folded.get("ts") or ""):
            folded["ts"] = ev["ts"]
    for folded in order:
        if folded["type"] == "record/delete":
            folded.pop("after", None)
    return order
