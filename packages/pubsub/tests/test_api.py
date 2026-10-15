import json
import uuid

from httpx import AsyncClient

from dbpersist_common.signing import EVENT_ID_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER, verify
from dbpersist_pubsub.testing import WebhookSink


def event(**over: object) -> dict:
    base = {
        "type_name": "Trade",
        "object_id": str(uuid.uuid4()),
        "key": "T1",
        "op": "insert",
        "version": 1,
        "data": {"trade_id": "T1", "qty": 150, "book": "A"},
    }
    base.update(over)
    return base


async def test_subscriber_lifecycle(client: AsyncClient, subscriber: dict) -> None:
    assert subscriber["secret"]
    r = await client.post("/v1/subscribers", json={"name": "app1", "webhook_url": "http://x.test/"})
    assert r.status_code == 409
    r = await client.get("/v1/subscribers")
    assert [s["name"] for s in r.json()] == ["app1"]
    r = await client.post(f"/v1/subscribers/{subscriber['id']}/heartbeat")
    assert r.status_code == 200
    r = await client.delete(f"/v1/subscribers/{subscriber['id']}")
    assert r.status_code == 204
    r = await client.get(f"/v1/subscribers/{subscriber['id']}")
    assert r.status_code == 404


async def test_subscription_validation(client: AsyncClient, subscriber: dict) -> None:
    base = f"/v1/subscribers/{subscriber['id']}/subscriptions"
    r = await client.post(base, json={"type": "Trade"})
    assert r.status_code == 422
    r = await client.post(base, json={"type": "Trade", "filter": {"field": "qty", "op": "gt"}})
    assert r.status_code == 422
    r = await client.post(
        base,
        json={"type": "Trade", "filter": {"field": "qty", "op": "gt", "value": 1}, "keys": ["a"]},
    )
    assert r.status_code == 422
    r = await client.post(base, json={"type": "Trade", "keys": ["T1", "T2"]})
    assert r.status_code == 201
    r = await client.get(base)
    assert len(r.json()) == 1 and r.json()[0]["keys"] == ["T1", "T2"]
    r = await client.delete(f"/v1/subscriptions/{r.json()[0]['id']}")
    assert r.status_code == 204


async def test_publish_filter_match_delivers_signed_webhook(
    client: AsyncClient, subscriber: dict, sink: WebhookSink, mem_stats
) -> None:
    base = f"/v1/subscribers/{subscriber['id']}/subscriptions"
    r = await client.post(
        base, json={"type": "Trade", "filter": {"field": "qty", "op": "gt", "value": 100}}
    )
    sub_id = r.json()["id"]
    # a second matching subscription for the same subscriber must not cause a second delivery
    await client.post(base, json={"type": "Trade", "keys": ["T1"]})
    # non-matching type and non-matching filter
    await client.post(
        base, json={"type": "Order", "filter": {"field": "qty", "op": "gt", "value": 0}}
    )

    r = await client.post("/v1/publish", json=event())
    assert r.status_code == 202, r.text
    r = await client.post("/v1/publish", json=event(key="T2", data={"trade_id": "T2", "qty": 5}))
    assert r.status_code == 202

    assert len(sink.requests) == 1
    req = sink.requests[0]
    body = json.loads(req.content)
    assert body["event"]["key"] == "T1"
    assert sub_id in body["subscription_ids"] and len(body["subscription_ids"]) == 2
    assert verify(
        subscriber["secret"],
        req.headers[TIMESTAMP_HEADER],
        req.content,
        req.headers[SIGNATURE_HEADER],
    )
    assert req.headers[EVENT_ID_HEADER] == body["event"]["event_id"]
    assert mem_stats.counts == {"received": 2, "matched": 1, "delivered": 1}

    r = await client.get("/v1/admin/clients")
    info = r.json()[0]
    assert (
        info["subscriptions"] == 3
        and info["pending"] == 0
        and info["last_delivery_ok_at"] is not None
    )
    r = await client.get("/v1/admin/stats")
    assert r.json()["delivered"] == 1 and r.json()["subscribers"] == 1


async def test_key_and_id_subscriptions(
    client: AsyncClient, subscriber: dict, sink: WebhookSink
) -> None:
    base = f"/v1/subscribers/{subscriber['id']}/subscriptions"
    obj_id = str(uuid.uuid4())
    await client.post(base, json={"type": "Trade", "keys": [obj_id], "by": "id"})
    await client.post("/v1/publish", json=event(object_id=obj_id, key="ZZ"))
    await client.post("/v1/publish", json=event(key="ZZ"))
    assert len(sink.requests) == 1


async def test_criteria_by_client_and_remove_idle(client: AsyncClient, subscriber: dict) -> None:
    base = f"/v1/subscribers/{subscriber['id']}/subscriptions"
    await client.post(
        base, json={"type": "Trade", "filter": {"field": "qty", "op": "gt", "value": 1}}
    )
    r = await client.get(f"/v1/admin/clients/{subscriber['id']}/criteria")
    assert r.status_code == 200 and r.json()[0]["filter"] == {
        "field": "qty",
        "op": "gt",
        "value": 1,
    }
    r = await client.post("/v1/admin/remove-idle", params={"idle_minutes": 60})
    assert r.json()["removed"] == []  # just created, not idle


async def test_replay_requeues_failed(
    client: AsyncClient, subscriber: dict, sink: WebhookSink
) -> None:
    base = f"/v1/subscribers/{subscriber['id']}/subscriptions"
    await client.post(base, json={"type": "Trade", "keys": ["T1"]})
    sink.fail_times = 100  # eager retries will exhaust
    await client.post("/v1/publish", json=event())
    attempts_first = len(sink.requests)
    assert attempts_first >= 1
    r = await client.get("/v1/admin/clients")
    assert r.json()[0]["failed"] == 1
    sink.fail_times = 0
    r = await client.post(
        f"/v1/subscribers/{subscriber['id']}/replay", params={"since": "2000-01-01T00:00:00Z"}
    )
    assert r.json()["requeued"] == 1
    assert len(sink.requests) == attempts_first + 1
    r = await client.get("/v1/admin/clients")
    assert r.json()[0]["failed"] == 0 and r.json()[0]["pending"] == 0


async def test_health_and_readiness(client: AsyncClient) -> None:
    assert (await client.get("/health")).status_code == 200
    r = await client.get("/healthz")
    assert r.status_code == 200, r.text
    assert r.json() == {"status": "ok", "database": "up", "broker": "up"}
