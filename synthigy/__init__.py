"""Synthigy SDK — thin, stdlib-only client for the /data endpoint.

ONE ENGINE: the async-native `AsyncClient` (async_client.py) is the whole
implementation. The blocking `Client` is a facade over it driving a single
background event-loop thread (facade.py) — same API as always for
scripts, seeds and notebooks; async apps use the `aconnect`/`aclient`
surface natively.

THE INVARIANT: one process, one client, one backend. `connect(...)` installs
a module-wide default (destroying any previous one, server-restart style);
the module-level verbs operate on it. Identity is multiplexed per-call via
`acting_as=`, never a second connect. Constructing `Client` directly is the
escape hatch (tests).

    import synthigy
    from synthigy import eq, rel

    synthigy.connect(endpoint="http://localhost:7887",
                     client_id="my-service", client_secret=SECRET)

    users = synthigy.search("User", {"active": eq(True), "_limit": 10},
                            {"name": None, "roles": {"name": None}},
                            acting_as=user_xid)

Async-first (BFFs, FastAPI/uvicorn services):

    synthigy.aconnect(endpoint=..., client_id=..., client_secret=SECRET)
    rows = await synthigy.aclient().search("User", ..., acting_as=user_xid)
"""


from . import ops  # noqa: F401
from .async_client import AsyncClient, PLATFORM_AUDIENCE  # noqa: F401
from .compose import compose_forest, compose_tree  # noqa: F401
from .errors import SynthigyError  # noqa: F401
from .facade import Client
from .filters import (  # noqa: F401
    and_, eq, gt, gte, ilike, in_, is_not_null, is_null, like, lt, lte, neq,
    nin, not_, or_,
)
from .selection import fields, normalize_selection, rel  # noqa: F401
from .util import new_xid  # noqa: F401

_default_client = None


def connect(endpoint, **config):
    """Create a client and install it as the module default. Any previous
    default is destroyed first (watches close, SSE drops)."""
    global _default_client
    prev = _default_client
    _default_client = Client(endpoint, **config)
    if prev:
        prev.close()
    return _default_client


def disconnect():
    """Destroy and uninstall the default client. No-op when not connected."""
    global _default_client
    prev = _default_client
    _default_client = None
    if prev:
        prev.close()


def get_client():
    """The installed default client, or None."""
    return _default_client


def _dflt():
    if _default_client is None:
        raise SynthigyError("not connected — call synthigy.connect() first",
                            "NOT_CONNECTED")
    return _default_client


# ── module-default ASYNC client (twin of connect/disconnect) ─────────────

_default_async_client = None


def aconnect(endpoint, **config):
    """Create an AsyncClient and install it as the module default — the
    async twin of connect(); same one-process-one-client invariant.
    Generated async typed ops (codegen) run on this default. A previous
    default is closed detachedly when an event loop is running."""
    global _default_async_client
    prev = _default_async_client
    _default_async_client = AsyncClient(endpoint, **config)
    if prev:
        import asyncio
        try:
            asyncio.get_running_loop()
            asyncio.ensure_future(prev.close())
        except RuntimeError:
            pass    # no loop — nothing live to tear down
    return _default_async_client


async def adisconnect():
    """Close and uninstall the default async client. No-op when absent."""
    global _default_async_client
    prev = _default_async_client
    _default_async_client = None
    if prev:
        await prev.close()


def aclient():
    """The installed default AsyncClient (raises NOT_CONNECTED if none)."""
    if _default_async_client is None:
        raise SynthigyError("not connected — call synthigy.aconnect() first",
                            "NOT_CONNECTED")
    return _default_async_client


# CRUD + XSQL
def search(*a, **kw): return _dflt().search(*a, **kw)
def get(*a, **kw): return _dflt().get(*a, **kw)
def sync(*a, **kw): return _dflt().sync(*a, **kw)
def stack(*a, **kw): return _dflt().stack(*a, **kw)
def slice(*a, **kw): return _dflt().slice(*a, **kw)  # noqa: A001
def delete(*a, **kw): return _dflt().delete(*a, **kw)
def purge(*a, **kw): return _dflt().purge(*a, **kw)
def sql_template(*a, **kw): return _dflt().sql_template(*a, **kw)
def query(*a, **kw): return _dflt().query(*a, **kw)
def search_tree(*a, **kw): return _dflt().search_tree(*a, **kw)
def get_tree(*a, **kw): return _dflt().get_tree(*a, **kw)
def exec_(*a, **kw): return _dflt().exec_(*a, **kw)


# Introspection
def schema(*a, **kw): return _dflt().schema(*a, **kw)
def lint(*a, **kw): return _dflt().lint(*a, **kw)
def onboard(*a, **kw): return _dflt().onboard(*a, **kw)
def onboard_complete(*a, **kw): return _dflt().onboard_complete(*a, **kw)
def deploy(*a, **kw): return _dflt().deploy(*a, **kw)
def destroy(*a, **kw): return _dflt().destroy(*a, **kw)
def deployed_model(*a, **kw): return _dflt().deployed_model(*a, **kw)
def runtime_model(*a, **kw): return _dflt().runtime_model(*a, **kw)
def token(*a, **kw): return _dflt().token(*a, **kw)
def history(): return _dflt().history


# Streaming
def listen(*a, **kw): return _dflt().listen(*a, **kw)
def observe(*a, **kw): return _dflt().observe(*a, **kw)


# Live data (watch layer)
def watch(*a, **kw): return _dflt().watch(*a, **kw)
def watch_schema(*a, **kw): return _dflt().watch_schema(*a, **kw)
def watch_query(*a, **kw): return _dflt().watch_query(*a, **kw)
def watch_query_xsql(*a, **kw): return _dflt().watch_query_xsql(*a, **kw)
def watch_sql_template(*a, **kw): return _dflt().watch_sql_template(*a, **kw)


# Subscriptions (advanced — prefer the watch family)
def subscribe(*a, **kw): return _dflt().subscribe(*a, **kw)
def unsubscribe(*a, **kw): return _dflt().unsubscribe(*a, **kw)
def subscribe_model(*a, **kw): return _dflt().subscribe_model(*a, **kw)
def unsubscribe_model(*a, **kw): return _dflt().unsubscribe_model(*a, **kw)
def set_subscriptions(*a, **kw): return _dflt().set_subscriptions(*a, **kw)
def clear_subscriptions(*a, **kw): return _dflt().clear_subscriptions(*a, **kw)
def subscriptions(*a, **kw): return _dflt().subscriptions(*a, **kw)
