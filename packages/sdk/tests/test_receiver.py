import json
import time
import uuid
from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from dbpersist_common.signing import SIGNATURE_HEADER, TIMESTAMP_HEADER, sign
from dbpersist_sdk import BaseCallbacks, DBPersistClient, ObjectRecord

SECRET = "topsecret"


class Recorder(BaseCallbacks):
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def on_insert(self, record: ObjectRecord) -> None:
        self.calls.append(("insert", record.key))

    async def on_update(self, record: ObjectRecord, changed_fields: list[str]) -> None:
        self.calls.append(("update", record.key, changed_fields))

    async def on_delete(self, record: ObjectRecord) -> None:
        self.calls.append(("delete", record.key))

    async def on_error(self, error: Exception) -> None:
        self.calls.append(("error", str(error)))


class Exploder(Recorder):
    async def on_insert(self, record: ObjectRecord) -> None:
        raise RuntimeError("bug")


@pytest.fixture
async def setup() -> AsyncIterator[tuple[AsyncClient, DBPersistClient]]:
    sdk = DBPersistClient("http://p", "http://s", "k", cache=True)
    sdk.use_subscriber(uuid.uuid4(), SECRET)
    app = FastAPI()
    app.include_router(sdk.receiver(), prefix="/dbpersist")  # type: ignore[attr-defined]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://me") as http:
        yield http, sdk
    await sdk.aclose()


def payload(op: str, sub_ids: list[uuid.UUID], changed: list[str] | None = None) -> bytes:
    ev = {
        "event_id": str(uuid.uuid4()),
        "occurred_at": "2026-01-01T00:00:00Z",
        "type_name": "Trade",
        "object_id": str(uuid.uuid4()),
        "key": "T1",
        "op": op,
        "version": 2,
        "data": {"trade_id": "T1", "qty": 5},
        "changed_fields": changed or [],
        "transaction_id": None,
    }
    return json.dumps(
        {"event": ev, "subscription_ids": [str(s) for s in sub_ids], "delivery_id": 1}
    ).encode()


def signed(body: bytes, secret: str = SECRET, ts: int | None = None) -> dict[str, str]:
    t = str(ts if ts is not None else int(time.time()))
    return {
        "Content-Type": "application/json",
        TIMESTAMP_HEADER: t,
        SIGNATURE_HEADER: sign(secret, t, body),
    }


async def test_dispatch_and_dedupe(setup: tuple[AsyncClient, DBPersistClient]) -> None:
    http, sdk = setup
    cb = Recorder()
    s1, s2, s3 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    sdk.callbacks[s1] = cb
    sdk.callbacks[s2] = cb  # same object twice -> one call
    other = Recorder()
    sdk.callbacks[s3] = other

    body = payload("insert", [s1, s2, s3, uuid.uuid4()])
    r = await http.post("/dbpersist/webhook", content=body, headers=signed(body))
    assert r.status_code == 200 and r.json() == {"dispatched": 2}
    assert cb.calls == [("insert", "T1")] and other.calls == [("insert", "T1")]
    assert sdk.cache[("Trade", "T1")].version == 2

    body = payload("update", [s1], ["qty"])
    await http.post("/dbpersist/webhook", content=body, headers=signed(body))
    assert cb.calls[-1] == ("update", "T1", ["qty"])

    body = payload("delete", [s1])
    await http.post("/dbpersist/webhook", content=body, headers=signed(body))
    assert cb.calls[-1] == ("delete", "T1") and ("Trade", "T1") not in sdk.cache


async def test_rejects_bad_signature_and_stale_timestamp(
    setup: tuple[AsyncClient, DBPersistClient],
) -> None:
    http, _ = setup
    body = payload("insert", [])
    r = await http.post("/dbpersist/webhook", content=body, headers=signed(body, secret="wrong"))
    assert r.status_code == 401
    r = await http.post(
        "/dbpersist/webhook", content=body, headers=signed(body, ts=int(time.time()) - 3600)
    )
    assert r.status_code == 401
    r = await http.post("/dbpersist/webhook", content=body)
    assert r.status_code == 401


async def test_callback_exception_reported_not_propagated(
    setup: tuple[AsyncClient, DBPersistClient],
) -> None:
    http, sdk = setup
    cb = Exploder()
    sid = uuid.uuid4()
    sdk.callbacks[sid] = cb
    body = payload("insert", [sid])
    r = await http.post("/dbpersist/webhook", content=body, headers=signed(body))
    assert r.status_code == 200
    assert cb.calls and cb.calls[0][0] == "error" and "bug" in cb.calls[0][1]


async def test_unregistered_returns_503() -> None:
    sdk = DBPersistClient("http://p", "http://s", "k")
    app = FastAPI()
    app.include_router(sdk.receiver())  # type: ignore[attr-defined]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://me") as http:
        r = await http.post("/webhook", content=b"{}")
    assert r.status_code == 503
    await sdk.aclose()
