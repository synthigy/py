import secrets
from datetime import datetime, timezone

BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def new_xid():
    """A fresh 22-char Base58 xid — client-minted identity for sync/stack,
    the same derivation the server uses (UUID bytes -> base58, left-padded
    with '1').

    Mint one before a write to know a record's id up front, or to make a
    retried write idempotent — the server accepts a caller-supplied id as-is,
    and the alternative (returning=True) costs the full echo on every write.
    """
    b = bytearray(secrets.token_bytes(16))
    # UUIDv4 bit layout, so the value round-trips the server's uuid->nanoid.
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    # Schoolbook base-256 -> base-58, the same conversion Bitcoin-style
    # base58 libraries use.
    digits = [0]
    for byte in b:
        carry = byte
        for i, d in enumerate(digits):
            x = d * 256 + carry
            digits[i] = x % 58
            carry = x // 58
        while carry > 0:
            digits.append(carry % 58)
            carry //= 58
    s = "".join(BASE58_ALPHABET[d] for d in reversed(digits))
    return "1" * max(0, 22 - len(s)) + s


def now_iso():
    """UTC now as an ISO-8601 Z string (history 'between' upper bounds)."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def generate_request_id():
    """16-char URL-safe id for X-Request-Id — readable in logs, enough
    entropy for one client's lifetime."""
    return secrets.token_urlsafe(12)


def xsql_document(source, op):
    """Ensure an XSQL operation DOCUMENT (STRICT wire: XSQL travels only as
    {op: "xsql", xsql: <document>}). Sources already starting with `@` pass
    through — their @verb is authoritative; bare rooted bodies get a
    synthetic `@<op> _q` header."""
    if source.lstrip().startswith("@"):
        return source
    return f"@{op} _q\n{source}"
