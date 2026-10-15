import hashlib
import hmac
import json
import time
from datetime import UTC, datetime

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy.exc import OperationalError

from dbpersist_persistence.services import inbound_queue as queue
from dbpersist_persistence.services.events import RecordingPublisher
from dbpersist_persistence.settings import Settings

SECRET = "hook-secret"
ANON = {"X-API-Key": ""}  # the ingress must not need the API key
H = {**ANON, "X-Hook-Secret": SECRET}

BROKER_SOURCE = {
    "name": "broker",
    "description": "clearing-firm event notifications",
    "mode": "upsert",
    "update_mode": "merge",
    "auto_register_types": True,
    "processing": "sync",
    "auth": {"mode": "header_secret", "secret": SECRET, "header": "X-Hook-Secret"},
    "mapping": {
        "events_path": "events",
        "type": "type",
        "type_map": {
            "account.opened": "BrokerAccount",
            "account.updated": "BrokerAccount",
            "account.closed": "BrokerAccount",
        },
        "key": {"account_id": "payload.accountId"},
        "op": "type",
        "op_map": {"account.opened": "insert", "account.closed": "delete"},
        "default_op": "update",
        "data": "payload",
        "external_id": "id",
        "occurred_at": "time",
        "envelope_field": "_source",
    },
}


def envelope(*events: dict) -> dict:
    return {"events": list(events)}


def ev(id_: str, type_: str, **payload: object) -> dict:
    return {"id": id_, "type": type_, "time": "2026-03-01T10:00:00Z", "payload": payload}


@pytest.fixture
def settings_fast(settings: Settings) -> Settings:
    return settings.model_copy(update={"inbound_max_attempts": 3, "inbound_backoff_max_seconds": 1})


async def _drain(client: AsyncClient, settings: Settings, publisher: RecordingPublisher) -> int:
    app = client._transport.app  # type: ignore[attr-defined]
    return await queue.drain_once(app.state.sessionmaker, settings, publisher)


# ---- management ------------------------------------------------------------------------------


async def test_source_crud_and_masking(client: AsyncClient) -> None:
    r = await client.post("/v1/inbound-sources", json=BROKER_SOURCE)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["auth"]["secret"] == "***" and body["ingest_path"] == "/v1/inbound/broker"
    assert body["mapping"]["type_map"]["account.closed"] == "BrokerAccount"
    assert body["queue"] == {"pending": 0, "done": 0, "failed": 0} and body["pull"] is None
    assert (await client.post("/v1/inbound-sources", json=BROKER_SOURCE)).status_code == 409
    assert (await client.get("/v1/inbound-sources/broker")).status_code == 200
    assert [s["name"] for s in (await client.get("/v1/inbound-sources")).json()] == ["broker"]
    bad = {**BROKER_SOURCE, "name": "x", "auth": {"mode": "header_secret"}}
    assert (await client.post("/v1/inbound-sources", json=bad)).status_code == 422
    upd = {**BROKER_SOURCE, "description": "changed", "enabled": False}
    r = await client.put("/v1/inbound-sources/broker", json=upd)
    assert r.status_code == 200 and r.json()["enabled"] is False
    assert (await client.delete("/v1/inbound-sources/broker")).status_code == 204
    assert (await client.get("/v1/inbound-sources/broker")).status_code == 404


async def test_ingress_auth_and_unknown(client: AsyncClient) -> None:
    await client.post("/v1/inbound-sources", json=BROKER_SOURCE)
    r = await client.post("/v1/inbound/broker", json=envelope(), headers=ANON)
    assert r.status_code == 401
    r = await client.post(
        "/v1/inbound/broker", json=envelope(), headers={**ANON, "X-Hook-Secret": "nope"}
    )
    assert r.status_code == 401
    r = await client.post("/v1/inbound/nope", json=envelope(), headers=H)
    assert r.status_code == 404
    r = await client.post("/v1/inbound/broker", content=b"{not json", headers=H)
    assert r.status_code == 400
    await client.put("/v1/inbound-sources/broker", json={**BROKER_SOURCE, "enabled": False})
    r = await client.post("/v1/inbound/broker", json=envelope(), headers=H)
    assert r.status_code == 404


# ---- synchronous processing ----------------------------------------------------------------


async def test_ingest_upsert_lifecycle(client: AsyncClient, publisher: RecordingPublisher) -> None:
    await client.post("/v1/inbound-sources", json=BROKER_SOURCE)

    # insert via auto-registered type, one unmapped type rejected, one key-less rejected
    body = envelope(
        ev("e1", "account.opened", accountId="A1", status="open", owner="pat"),
        ev("e2", "order.filled", orderId="O1"),
        {"id": "e3", "type": "account.updated", "payload": {"status": "x"}},
    )
    r = await client.post("/v1/inbound/broker", json=body, headers=H)
    assert r.status_code == 200, r.text
    out = r.json()
    assert (out["received"], out["accepted"], out["rejected"], out["duplicates"]) == (3, 1, 2, 0)
    assert out["events"][0]["status"] == "accepted" and out["events"][0]["op"] == "insert"
    assert "not mapped" in out["events"][1]["reason"]
    assert "key field" in out["events"][2]["reason"]
    t = await client.get("/v1/types/BrokerAccount")
    assert t.status_code == 200 and t.json()["key_fields"] == ["account_id"]
    obj = (await client.get("/v1/objects/BrokerAccount/by-key/A1")).json()
    assert obj["data"]["owner"] == "pat" and obj["data"]["account_id"] == "A1"
    assert obj["data"]["_source"]["type"] == "account.opened"
    assert publisher.events[-1].op == "insert" and publisher.events[-1].key == "A1"

    # redelivery of e1 is a duplicate; e4 merges and reports changed fields
    body = envelope(
        ev("e1", "account.opened", accountId="A1", status="open", owner="pat"),
        ev("e4", "account.updated", accountId="A1", status="restricted"),
    )
    n_before = len(publisher.events)
    out = (await client.post("/v1/inbound/broker", json=body, headers=H)).json()
    assert (out["accepted"], out["duplicates"]) == (1, 1)
    assert out["events"][0]["status"] == "duplicate"
    assert len(publisher.events) == n_before + 1
    assert publisher.events[-1].op == "update" and "status" in publisher.events[-1].changed_fields
    obj = (await client.get("/v1/objects/BrokerAccount/by-key/A1")).json()
    assert obj["data"]["status"] == "restricted" and obj["data"]["owner"] == "pat"
    assert obj["version"] == 2

    # unchanged update emits nothing; delete soft-deletes and emits delete
    n_before = len(publisher.events)
    body = envelope(
        ev("e5", "account.updated", accountId="A1", status="restricted"),
        ev("e6", "account.closed", accountId="A1"),
        ev("e7", "account.closed", accountId="ZZ"),
    )
    out = (await client.post("/v1/inbound/broker", json=body, headers=H)).json()
    assert [e["status"] for e in out["events"]] == ["unchanged", "accepted", "unchanged"]
    assert len(publisher.events) == n_before + 1 and publisher.events[-1].op == "delete"
    assert (await client.get("/v1/objects/BrokerAccount/by-key/A1")).status_code == 404

    src = (await client.get("/v1/inbound-sources/broker")).json()
    assert src["received_count"] == 8 and src["accepted_count"] == 3
    assert src["duplicate_count"] == 1 and src["rejected_count"] == 2
    receipts = (await client.get("/v1/inbound-sources/broker/receipts")).json()
    assert {r_["external_id"] for r_ in receipts} == {"e1", "e4", "e5", "e6", "e7"}


async def test_ingest_publish_mode_and_hmac(
    client: AsyncClient, publisher: RecordingPublisher
) -> None:
    await client.post("/v1/types", json={"name": "Quote", "key_fields": ["symbol"]})
    src = {
        "name": "quotes",
        "mode": "publish",
        "processing": "sync",
        "auth": {
            "mode": "hmac_sha256",
            "secret": "k",
            "header": "X-Signature",
            "signature_prefix": "sha256=",
            "timestamp_header": "X-Timestamp",
            "signed_payload": "{timestamp}.{body}",
        },
        "mapping": {"type": "const:Quote", "default_type": "Quote", "external_id": "seq"},
    }
    assert (await client.post("/v1/inbound-sources", json=src)).status_code == 201
    payload = {"seq": "1001", "symbol": "ACME", "bid": 10.1, "ask": 10.2}
    raw = json.dumps(payload).encode()
    ts = str(int(time.time()))
    sig = "sha256=" + hmac.new(b"k", f"{ts}.".encode() + raw, hashlib.sha256).hexdigest()
    headers = {**ANON, "X-Signature": sig, "X-Timestamp": ts, "Content-Type": "application/json"}
    r = await client.post("/v1/inbound/quotes", content=raw, headers=headers)
    assert r.status_code == 200, r.text
    assert r.json()["accepted"] == 1
    assert publisher.events[-1].type_name == "Quote" and publisher.events[-1].key == "ACME"
    assert publisher.events[-1].object_id is None  # nothing persisted
    assert (await client.post("/v1/objects/Quote/count", json={})).json()["count"] == 0
    r = await client.post(
        "/v1/inbound/quotes", content=raw, headers={**headers, "X-Signature": "sha256=00"}
    )
    assert r.status_code == 401
    r = await client.post("/v1/inbound/quotes", content=raw + b" ", headers=headers)
    assert r.status_code == 401  # body differs from what was signed


async def test_dry_run(client: AsyncClient) -> None:
    await client.post("/v1/inbound-sources", json=BROKER_SOURCE)
    payload = envelope(ev("e9", "account.opened", accountId="A9"), ev("e10", "unknown.kind"))
    r = await client.post(
        "/v1/inbound-sources/broker/test",
        json={"payload": payload, "headers": {"X-Hook-Secret": "bad"}},
    )
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["auth_ok"] is False and out["auth_error"] == "bad secret"
    assert [e["type_name"] for e in out["events"]] == ["BrokerAccount"]
    assert out["problems"][0]["index"] == 1
    assert out["unknown_types"] == ["BrokerAccount"]  # dry run registers nothing
    assert (await client.get("/v1/types/BrokerAccount")).status_code == 404
    r = await client.post(
        "/v1/inbound-sources/broker/test",
        json={"payload": payload, "headers": {"X-Hook-Secret": SECRET}},
    )
    assert r.json()["auth_ok"] is True


# ---- the durable queue ---------------------------------------------------------------------


async def test_queue_mode_stores_then_processes_in_order(
    client: AsyncClient, publisher: RecordingPublisher, settings_fast: Settings
) -> None:
    await client.post("/v1/inbound-sources", json={**BROKER_SOURCE, "processing": "queue"})
    r1 = await client.post(
        "/v1/inbound/broker",
        json=envelope(ev("q1", "account.opened", accountId="B1", status="open")),
        headers=H,
    )
    r2 = await client.post(
        "/v1/inbound/broker",
        json=envelope(ev("q2", "account.updated", accountId="B1", status="closed")),
        headers=H,
    )
    assert r1.status_code == 202 and r2.status_code == 202, (r1.text, r2.text)
    assert r1.json()["queued"] is True and r2.json()["message_id"] > r1.json()["message_id"]
    # nothing applied yet
    assert publisher.events == []
    assert (await client.get("/v1/types/BrokerAccount")).status_code == 404
    src = (await client.get("/v1/inbound-sources/broker")).json()
    assert src["queue"]["pending"] == 2

    assert await _drain(client, settings_fast, publisher) == 2
    assert [e.op for e in publisher.events] == ["insert", "update"]  # arrival order kept
    obj = (await client.get("/v1/objects/BrokerAccount/by-key/B1")).json()
    assert obj["data"]["status"] == "closed" and obj["version"] == 2
    msgs = (await client.get("/v1/inbound-sources/broker/messages")).json()
    assert [m["status"] for m in msgs] == ["done", "done"]
    assert msgs[-1]["result"]["accepted"] == 1 and msgs[0]["origin"] == "push"
    assert await _drain(client, settings_fast, publisher) == 0  # nothing left


async def test_queue_retries_transient_failures_then_quarantines(
    client: AsyncClient,
    publisher: RecordingPublisher,
    settings_fast: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await client.post("/v1/inbound-sources", json={**BROKER_SOURCE, "processing": "queue"})
    await client.post(
        "/v1/inbound/broker",
        json=envelope(ev("t1", "account.opened", accountId="C1", status="open")),
        headers=H,
    )
    await client.post(
        "/v1/inbound/broker",
        json=envelope(ev("t2", "account.opened", accountId="C2", status="open")),
        headers=H,
    )

    calls = {"n": 0}
    real = queue.ingest

    async def flaky(session, source, body):  # fails the first two attempts of the first message
        calls["n"] += 1
        if calls["n"] <= 2:
            raise OperationalError("SELECT 1", {}, Exception("connection reset"))
        return await real(session, source, body)

    monkeypatch.setattr(queue, "ingest", flaky)

    assert (
        await _drain(client, settings_fast, publisher) == 1
    )  # deferred, second message waits (order)
    msgs = (await client.get("/v1/inbound-sources/broker/messages?status=pending")).json()
    assert (
        len(msgs) == 2
        and msgs[-1]["attempts"] == 1
        and "connection reset" in msgs[-1]["last_error"]
    )
    assert publisher.events == []

    # make the deferred message due again and drain: second failure, then success, then message 2
    app = client._transport.app  # type: ignore[attr-defined]
    async with app.state.sessionmaker() as s:
        await s.execute(
            __import__("sqlalchemy").text("UPDATE inbound_messages SET next_attempt_at = now()")
        )
        await s.commit()
    await _drain(client, settings_fast, publisher)
    async with app.state.sessionmaker() as s:
        await s.execute(
            __import__("sqlalchemy").text("UPDATE inbound_messages SET next_attempt_at = now()")
        )
        await s.commit()
    await _drain(client, settings_fast, publisher)
    assert [e.key for e in publisher.events] == ["C1", "C2"]
    assert calls["n"] == 4

    # a message that keeps failing is quarantined after inbound_max_attempts; the next proceeds
    calls["n"] = -10_000
    await client.post(
        "/v1/inbound/broker",
        json=envelope(ev("t3", "account.opened", accountId="C3", status="open")),
        headers=H,
    )
    await client.post(
        "/v1/inbound/broker",
        json=envelope(ev("t4", "account.opened", accountId="C4", status="open")),
        headers=H,
    )
    # the second message only starts failing once the first is quarantined (order is kept)
    for _ in range(settings_fast.inbound_max_attempts * 2):
        async with app.state.sessionmaker() as s:
            await s.execute(
                __import__("sqlalchemy").text(
                    "UPDATE inbound_messages SET next_attempt_at = now() WHERE status = 'pending'"
                )
            )
            await s.commit()
        await _drain(client, settings_fast, publisher)
    failed = (await client.get("/v1/inbound-sources/broker/messages?status=failed")).json()
    assert len(failed) == 2 and all(
        m["attempts"] == settings_fast.inbound_max_attempts for m in failed
    )
    src = (await client.get("/v1/inbound-sources/broker")).json()
    assert src["queue"]["failed"] == 2 and src["queue"]["pending"] == 0

    # operator retry puts them back; with the fault gone they go through
    monkeypatch.setattr(queue, "ingest", real)
    r = await client.post("/v1/inbound-sources/broker/messages/retry")
    assert r.json()["requeued"] == 2
    await _drain(client, settings_fast, publisher)
    assert [e.key for e in publisher.events] == ["C1", "C2", "C3", "C4"]


async def test_pull_feed_with_cursor(
    client: AsyncClient, publisher: RecordingPublisher, settings_fast: Settings
) -> None:
    pages = {
        None: {"events": [ev("p1", "account.opened", accountId="P1", status="open")], "next": "c2"},
        "c2": {
            "events": [ev("p2", "account.updated", accountId="P1", status="closed")],
            "next": "c3",
        },
        "c3": {"events": [], "next": "c3"},
    }
    seen: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            {
                "cursor": request.url.params.get("cursor"),
                "auth": request.headers.get("Authorization"),
            }
        )
        return httpx.Response(200, json=pages[request.url.params.get("cursor")])

    app = client._transport.app  # type: ignore[attr-defined]
    app.state.inbound_client_factory = lambda: httpx.AsyncClient(
        transport=httpx.MockTransport(handler)
    )

    src = {
        **BROKER_SOURCE,
        "processing": "queue",
        "pull": {
            "url": "https://broker.test/events",
            "headers": {"Authorization": "Bearer t"},
            "interval_seconds": 5,
            "cursor_param": "cursor",
            "cursor_path": "next",
        },
    }
    assert (await client.post("/v1/inbound-sources", json=src)).status_code == 201

    r = await client.post("/v1/inbound-sources/broker/pull")
    assert r.status_code == 200 and r.json()["pulled"] is True and r.json()["pull_cursor"] == "c2"
    assert seen[0] == {"cursor": None, "auth": "Bearer t"}
    r = await client.post("/v1/inbound-sources/broker/pull")
    assert r.json()["pull_cursor"] == "c3" and seen[1]["cursor"] == "c2"
    await _drain(client, settings_fast, publisher)
    assert [e.op for e in publisher.events] == ["insert", "update"]
    msgs = (await client.get("/v1/inbound-sources/broker/messages")).json()
    assert {m["origin"] for m in msgs} == {"pull"}

    # the scheduled pull respects the interval: nothing new right after a pull
    factory = app.state.inbound_client_factory
    assert await queue.pull_due(app.state.sessionmaker, factory) == 0
    async with app.state.sessionmaker() as s:
        await s.execute(
            __import__("sqlalchemy").text(
                "UPDATE inbound_sources SET last_pulled_at = now() - interval '1 hour'"
            )
        )
        await s.commit()
    assert await queue.pull_due(app.state.sessionmaker, factory) == 1
    assert seen[2]["cursor"] == "c3"
    # an overlapping push of p2 is deduped against the pulled copy
    r = await client.post(
        "/v1/inbound/broker",
        json=envelope(ev("p2", "account.updated", accountId="P1", status="closed")),
        headers=H,
    )
    await _drain(client, settings_fast, publisher)
    msgs = (await client.get("/v1/inbound-sources/broker/messages?status=done")).json()
    assert msgs[0]["result"]["duplicates"] == 1


async def test_pull_failure_is_recorded(client: AsyncClient) -> None:
    app = client._transport.app  # type: ignore[attr-defined]
    app.state.inbound_client_factory = lambda: httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(503, text="down"))
    )
    src = {**BROKER_SOURCE, "pull": {"url": "https://broker.test/events", "cursor_path": "next"}}
    await client.post("/v1/inbound-sources", json=src)
    r = await client.post("/v1/inbound-sources/broker/pull")
    assert r.json()["pulled"] is False and "503" in r.json()["last_pull_error"]
    assert (await client.get("/v1/inbound-sources/broker")).json()["queue"]["pending"] == 0
    assert (
        await client.post("/v1/inbound-sources/broker/pull", headers={"X-API-Key": "wrong"})
    ).status_code == 401
    before = datetime.now(UTC)
    assert (await client.get("/v1/inbound-sources/broker")).json()["last_pulled_at"] is not None
    assert before.year >= 2026
