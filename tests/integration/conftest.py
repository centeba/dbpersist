"""Integration harness: runs the scenario tests against live services.

Requires persistence (PERSIST_URL), pubsub (PUBSUB_URL) and a Celery worker to be running.
The whole directory is skipped when the services are not reachable, so `uv run pytest`
stays green on a machine without the stack.

A webhook receiver is started in-process on WEBHOOK_PORT (default 9101), bound to all
interfaces, and advertised to pubsub as WEBHOOK_HOST (default host.docker.internal so a
worker inside Docker can reach it; use 127.0.0.1 when the worker runs on the host).
"""

import asyncio
import os
import socket
import time
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx
import pytest
import pytest_asyncio
import uvicorn
from fastapi import FastAPI, Request, Response

from dbpersist_sdk import BaseCallbacks, DBPersistClient, ObjectRecord

PERSIST_URL = os.environ.get("PERSIST_URL", "http://localhost:8101")
PUBSUB_URL = os.environ.get("PUBSUB_URL", "http://localhost:8102")
API_KEY = os.environ.get("API_KEY", "dev-api-key")
WEBHOOK_PORT = int(os.environ.get("WEBHOOK_PORT", "9101"))
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST", "host.docker.internal")
DELIVERY_TIMEOUT = float(os.environ.get("DELIVERY_TIMEOUT", "20"))
NEGATIVE_WAIT = float(os.environ.get("NEGATIVE_WAIT", "2"))

HEADERS = {"X-API-Key": API_KEY}

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


def _reachable(url: str) -> bool:
    try:
        return httpx.get(f"{url}/health", timeout=2).status_code == 200
    except httpx.HTTPError:
        return False


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    here = os.path.dirname(__file__)
    mine = [i for i in items if str(i.fspath).startswith(here)]
    if not mine:
        return
    if not (_reachable(PERSIST_URL) and _reachable(PUBSUB_URL)):
        skip = pytest.mark.skip(reason=f"services not reachable at {PERSIST_URL} / {PUBSUB_URL}")
        for i in mine:
            i.add_marker(skip)


def unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


class Collector(BaseCallbacks):
    """Records every callback and wakes waiters."""

    def __init__(self) -> None:
        self.events: list[tuple[str, ObjectRecord, list[str]]] = []
        self.errors: list[Exception] = []
        self._changed = asyncio.Event()

    def _push(self, kind: str, record: ObjectRecord, changed: list[str] | None = None) -> None:
        self.events.append((kind, record, changed or []))
        self._changed.set()

    async def on_insert(self, record: ObjectRecord) -> None:
        self._push("insert", record)

    async def on_update(self, record: ObjectRecord, changed_fields: list[str]) -> None:
        self._push("update", record, changed_fields)

    async def on_delete(self, record: ObjectRecord) -> None:
        self._push("delete", record)

    async def on_error(self, error: Exception) -> None:
        self.errors.append(error)
        self._changed.set()

    def of(self, kind: str) -> list[tuple[str, ObjectRecord, list[str]]]:
        return [e for e in self.events if e[0] == kind]

    def keys(self, kind: str) -> list[str]:
        return [e[1].key for e in self.of(kind)]

    async def wait_for(
        self, predicate: Callable[[], bool], what: str, timeout: float = DELIVERY_TIMEOUT
    ) -> None:
        deadline = time.monotonic() + timeout
        while not predicate():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"timed out waiting for {what}; got {self.summary()}")
            self._changed.clear()
            try:
                await asyncio.wait_for(self._changed.wait(), timeout=min(remaining, 0.5))
            except TimeoutError:
                pass

    async def expect_none(self, kind: str, key: str, wait: float = NEGATIVE_WAIT) -> None:
        await asyncio.sleep(wait)
        assert key not in self.keys(kind), f"unexpected {kind} for {key}: {self.summary()}"

    def summary(self) -> list[tuple[str, str, int, list[str]]]:
        return [(k, r.key, r.version, c) for k, r, c in self.events]


class Receiver:
    """One uvicorn server hosting the webhook routers of every SDK client in the session.

    ``flaky_failures`` makes the next N requests on ``/flaky/...`` return HTTP 500, which is
    how the retry scenario provokes delivery failures.
    """

    def __init__(self) -> None:
        self.app = FastAPI()
        self.flaky_failures = 0
        self.flaky_hits = 0
        self._server: uvicorn.Server | None = None
        self._task: asyncio.Task[None] | None = None

        @self.app.middleware("http")
        async def _flaky(request: Request, call_next: Any) -> Response:
            if request.url.path.startswith("/flaky/"):
                self.flaky_hits += 1
                if self.flaky_failures > 0:
                    self.flaky_failures -= 1
                    return Response("induced failure", status_code=500)
            return await call_next(request)

    def mount(self, client: DBPersistClient, prefix: str) -> str:
        self.app.include_router(client.receiver(), prefix=prefix)  # type: ignore[attr-defined]
        return f"http://{WEBHOOK_HOST}:{WEBHOOK_PORT}{prefix}/webhook"

    async def start(self) -> None:
        config = uvicorn.Config(self.app, host="0.0.0.0", port=WEBHOOK_PORT, log_level="warning")
        self._server = uvicorn.Server(config)
        self._task = asyncio.create_task(self._server.serve())
        while not self._server.started:
            await asyncio.sleep(0.05)

    async def stop(self) -> None:
        if self._server and self._task:
            self._server.should_exit = True
            await self._task


def _port_free(port: int) -> bool:
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def receiver() -> AsyncIterator[Receiver]:
    if not _port_free(WEBHOOK_PORT):
        pytest.skip(f"webhook port {WEBHOOK_PORT} is in use")
    r = Receiver()
    # Routers are mounted lazily by the sdk fixtures below *before* the first request, but
    # FastAPI allows include_router after startup, so start now and mount later.
    await r.start()
    yield r
    await r.stop()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def sdk(receiver: Receiver) -> AsyncIterator[DBPersistClient]:
    """Primary client: registered subscriber with the receiver mounted at /primary."""
    client = DBPersistClient(PERSIST_URL, PUBSUB_URL, API_KEY, subscriber_name=unique("it-primary"))
    client.webhook_url = receiver.mount(client, "/primary")
    await client.register_subscriber()
    yield client
    try:
        await client.unregister_subscriber()
    finally:
        await client.aclose()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def flaky_sdk(receiver: Receiver) -> AsyncIterator[DBPersistClient]:
    """Second subscriber whose webhook path is subject to induced failures."""
    client = DBPersistClient(PERSIST_URL, PUBSUB_URL, API_KEY, subscriber_name=unique("it-flaky"))
    client.webhook_url = receiver.mount(client, "/flaky")
    await client.register_subscriber()
    yield client
    try:
        await client.unregister_subscriber()
    finally:
        await client.aclose()


@pytest_asyncio.fixture(loop_scope="session")
async def persist() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=f"{PERSIST_URL}/v1", headers=HEADERS, timeout=30) as c:
        yield c


@pytest_asyncio.fixture(loop_scope="session")
async def pubsub() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=f"{PUBSUB_URL}/v1", headers=HEADERS, timeout=30) as c:
        yield c


TRADE_SCHEMA = {
    "type": "object",
    "required": ["trade_id", "qty"],
    "properties": {
        "trade_id": {"type": "string"},
        "qty": {"type": "number"},
        "book": {"type": ["string", "null"]},
        "account": {"type": ["string", "null"], "x-ref": "__ACCOUNT__"},
    },
}


@pytest_asyncio.fixture(loop_scope="session")
async def trade_type(sdk: DBPersistClient) -> str:
    """A fresh Trade-like type per test (and a matching Account type for x-ref expansion)."""
    acct = unique("Account")
    await sdk.register_type(acct, ["acct"])
    name = unique("Trade")
    schema = dict(TRADE_SCHEMA)
    schema["properties"] = {
        **TRADE_SCHEMA["properties"],
        "account": {"type": ["string", "null"], "x-ref": acct},
    }
    await sdk.register_type(name, ["trade_id"], schema)
    return name


@pytest_asyncio.fixture(loop_scope="session")
async def account_type_of(sdk: DBPersistClient, trade_type: str) -> str:
    info = await sdk.get_type(trade_type)
    return str(info.json_schema["properties"]["account"]["x-ref"])
