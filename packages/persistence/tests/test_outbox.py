"""Transactional outbox: event rows live and die with their write; the relay sends them in order."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, update

from dbpersist_common.events import ObjectEvent
from dbpersist_persistence.main import create_app
from dbpersist_persistence.models import OutboxEvent
from dbpersist_persistence.services import outbox
from dbpersist_persistence.services.events import RecordingPublisher
from dbpersist_persistence.settings import Settings

from .conftest import HEADERS, relay


class FailingPublisher:
    """Records events but raises for the keys in ``fail_keys``: a broker outage for some sends."""

    def __init__(self, fail_keys: set[str]) -> None:
        self.fail_keys = fail_keys
        self.events: list[ObjectEvent] = []

    def publish(self, events: list[ObjectEvent]) -> None:
        for ev in events:
            if ev.key in self.fail_keys:
                raise ConnectionError(f"broker unreachable for {ev.key}")
            self.events.append(ev)


@pytest.fixture
async def raw(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """API client that does *not* relay the outbox after each call."""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test", headers=HEADERS
    ) as c:
        await c.post("/v1/types", json={"name": "Trade", "key_fields": ["trade_id"]})
        yield c


async def rows(app: FastAPI) -> list[OutboxEvent]:
    async with app.state.sessionmaker() as session:
        res = await session.execute(select(OutboxEvent).order_by(OutboxEvent.id))
        return list(res.scalars().all())


async def test_event_row_is_written_in_the_write_transaction(
    app: FastAPI, raw: AsyncClient, publisher: RecordingPublisher
) -> None:
    r = await raw.post("/v1/objects/Trade", json={"trade_id": "T1", "qty": 1})
    assert r.status_code == 201, r.text
    assert publisher.events == []  # nothing is sent on the request path any more
    (row,) = await rows(app)
    assert row.sent_at is None and row.attempts == 0 and row.last_error is None
    ev = ObjectEvent.model_validate(row.event)
    assert (ev.type_name, ev.key, ev.op, ev.version) == ("Trade", "T1", "insert", 1)
    assert str(ev.object_id) == r.json()["id"]

    assert await relay(app) == 1
    (row,) = await rows(app)
    assert row.sent_at is not None and row.attempts == 1
    assert [e.event_id for e in publisher.events] == [ev.event_id]
    assert await relay(app) == 0


async def test_failed_writes_stage_no_event(app: FastAPI, raw: AsyncClient) -> None:
    first = (await raw.post("/v1/objects/Trade", json={"trade_id": "T1", "qty": 1})).json()
    await relay(app)
    oid = first["id"]

    r = await raw.patch(f"/v1/objects/Trade/{oid}", json={"version": 9, "data": {"qty": 2}})
    assert r.status_code == 409 and r.json()["code"] == "version_conflict"
    r = await raw.delete(f"/v1/objects/Trade/{oid}", params={"version": 9})
    assert r.status_code == 409
    r = await raw.post("/v1/objects/Trade", json={"trade_id": "T1", "qty": 1})
    assert r.status_code == 409 and r.json()["code"] == "duplicate_key"
    r = await raw.post(
        "/v1/transactions",
        json={
            "ops": [
                {"op": "insert", "type": "Trade", "data": {"trade_id": "T2", "qty": 1}},
                {"op": "update", "type": "Trade", "id": oid, "version": 9, "data": {"qty": 3}},
            ]
        },
    )
    assert r.status_code == 409  # the whole transaction rolled back, including T2
    r = await raw.post(
        "/v1/transactions",
        json={
            "ops": [
                {"op": "insert", "type": "Trade", "data": {"trade_id": "T3", "qty": 1}},
                {"op": "insert", "type": "Trade", "data": {"trade_id": "T3", "qty": 2}},
            ]
        },
    )
    assert r.status_code == 409
    assert (await raw.post("/v1/objects/Trade/count", json={})).json()["count"] == 1

    assert [r_.sent_at is None for r_ in await rows(app)] == [False]  # only the first insert
    assert await relay(app) == 0


async def test_relay_sends_in_id_order_and_marks_sent(
    app: FastAPI, raw: AsyncClient, publisher: RecordingPublisher
) -> None:
    oid = (await raw.post("/v1/objects/Trade", json={"trade_id": "T1", "qty": 1})).json()["id"]
    await raw.patch(f"/v1/objects/Trade/{oid}", json={"version": 1, "data": {"qty": 2}})
    await raw.post("/v1/objects/Trade", json={"trade_id": "T2", "qty": 1})
    await raw.put(f"/v1/objects/Trade/{oid}", json={"version": 2, "data": {"trade_id": "T1"}})
    r = await raw.delete(f"/v1/objects/Trade/{oid}", params={"version": 3})
    assert r.status_code == 204
    assert all(r_.sent_at is None for r_ in await rows(app))

    assert await relay(app) == 5
    got = [(e.key, e.op, e.version) for e in publisher.events]
    assert got == [
        ("T1", "insert", 1),
        ("T1", "update", 2),
        ("T2", "insert", 1),
        ("T1", "update", 3),
        ("T1", "delete", 4),
    ]
    staged = await rows(app)
    assert all(r_.sent_at is not None and r_.attempts == 1 for r_ in staged)
    assert [ObjectEvent.model_validate(r_.event).event_id for r_ in staged] == [
        e.event_id for e in publisher.events
    ]


async def test_relay_failure_leaves_rows_unsent_and_retries(app: FastAPI, raw: AsyncClient) -> None:
    for key in ("A", "B", "C"):
        await raw.post("/v1/objects/Trade", json={"trade_id": key, "qty": 1})
    maker = app.state.sessionmaker

    failing = FailingPublisher(fail_keys={"B"})
    with pytest.raises(ConnectionError):
        await outbox.relay_once(maker, failing, batch_size=100)
    assert [e.key for e in failing.events] == ["A"]
    a, b, c = await rows(app)
    assert a.sent_at is not None and a.attempts == 1
    assert b.sent_at is None and b.attempts == 1 and "broker unreachable" in (b.last_error or "")
    assert c.sent_at is None and c.attempts == 0 and c.last_error is None

    # the outage is over: the unsent rows go out in order, the sent one is not repeated
    recovered = RecordingPublisher()
    assert await outbox.relay_once(maker, recovered, batch_size=100) == 2
    assert [e.key for e in recovered.events] == ["B", "C"]
    a, b, c = await rows(app)
    assert b.sent_at is not None and b.attempts == 2 and b.last_error is None
    assert c.sent_at is not None and c.attempts == 1
    assert await outbox.relay_once(maker, recovered, batch_size=100) == 0


async def test_relay_batches(app: FastAPI, raw: AsyncClient, publisher: RecordingPublisher) -> None:
    for i in range(5):
        await raw.post("/v1/objects/Trade", json={"trade_id": f"T{i}", "qty": 1})
    maker = app.state.sessionmaker
    assert await outbox.relay_once(maker, publisher, batch_size=2) == 2
    assert await outbox.relay_once(maker, publisher, batch_size=2) == 2
    assert await outbox.relay_once(maker, publisher, batch_size=2) == 1
    assert [e.key for e in publisher.events] == [f"T{i}" for i in range(5)]


async def test_publish_only_goes_through_the_outbox(
    app: FastAPI, raw: AsyncClient, publisher: RecordingPublisher
) -> None:
    r = await raw.post(
        "/v1/publish",
        json={"type": "Trade", "op": "update", "data": {"trade_id": "T1", "qty": 3}},
    )
    assert r.status_code == 202, r.text
    (row,) = await rows(app)
    ev = ObjectEvent.model_validate(row.event)
    assert row.sent_at is None and ev.object_id is None and ev.key == "T1"
    assert str(ev.event_id) == r.json()["event_id"]
    assert await relay(app) == 1
    assert publisher.events[-1].event_id == ev.event_id


async def test_prune_removes_only_old_sent_rows(app: FastAPI, raw: AsyncClient) -> None:
    for key in ("A", "B"):
        await raw.post("/v1/objects/Trade", json={"trade_id": key, "qty": 1})
    await relay(app)
    await raw.post("/v1/objects/Trade", json={"trade_id": "C", "qty": 1})  # unsent
    maker = app.state.sessionmaker
    async with maker() as session:
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.sent_at.is_not(None))
            .values(sent_at=datetime.now(UTC) - timedelta(hours=48))
        )
        await session.commit()
    assert await outbox.prune_sent(maker, retention_hours=72) == 0
    assert await outbox.prune_sent(maker, retention_hours=24) == 2
    (c,) = await rows(app)
    assert ObjectEvent.model_validate(c.event).key == "C" and c.sent_at is None


async def test_relay_loop_runs_in_the_api_process(app: FastAPI, settings: Settings) -> None:
    """With OUTBOX_RELAY_ENABLED the lifespan starts the loop and committed events get sent."""
    live = settings.model_copy(update={"outbox_relay_enabled": True, "outbox_tick_seconds": 0.05})
    publisher = RecordingPublisher()
    live_app = create_app(live, publisher=publisher)
    async with live_app.router.lifespan_context(live_app):
        async with AsyncClient(
            transport=ASGITransport(app=live_app), base_url="http://test", headers=HEADERS
        ) as c:
            await c.post("/v1/types", json={"name": "Live", "key_fields": ["k"]})
            r = await c.post("/v1/objects/Live", json={"k": "x"})
            assert r.status_code == 201
            for _ in range(100):
                (row,) = await rows(app)
                if row.sent_at is not None:
                    break
                await asyncio.sleep(0.05)
    assert row.sent_at is not None and row.attempts == 1
    assert [e.key for e in publisher.events] == ["x"]
