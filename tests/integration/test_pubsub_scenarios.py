"""Pub/sub scenarios S1-S11 (see docs/TEST_CASES.md) against the live services and worker."""

import asyncio
import uuid

import httpx
import pytest

from dbpersist_sdk import DBPersistClient, DuplicateKey

from .conftest import Collector, Receiver, unique

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def _client_info(pubsub: httpx.AsyncClient, subscriber_id: uuid.UUID) -> dict:
    r = await pubsub.get("/admin/clients")
    r.raise_for_status()
    return next(c for c in r.json() if c["id"] == str(subscriber_id))


# S1 ------------------------------------------------------------------------------------------
async def test_s1_subscriber_lifecycle(pubsub: httpx.AsyncClient) -> None:
    name = unique("s1")
    r = await pubsub.post(
        "/subscribers", json={"name": name, "webhook_url": "http://example.test/hook"}
    )
    assert r.status_code == 201 and r.json()["secret"]
    sid = r.json()["id"]
    r = await pubsub.post(
        "/subscribers", json={"name": name, "webhook_url": "http://example.test/hook"}
    )
    assert r.status_code == 409
    r = await pubsub.post("/subscribers", json={"name": unique("s1"), "webhook_url": "not a url"})
    assert r.status_code == 422

    before = (await pubsub.get(f"/subscribers/{sid}")).json()["last_seen_at"]
    await asyncio.sleep(0.05)
    r = await pubsub.post(f"/subscribers/{sid}/heartbeat")
    assert r.status_code == 200 and r.json()["last_seen_at"] > before

    r = await pubsub.post(f"/subscribers/{sid}/subscriptions", json={"type": "X", "keys": ["k"]})
    sub_id = r.json()["id"]
    assert (await pubsub.delete(f"/subscribers/{sid}")).status_code == 204
    assert (await pubsub.get(f"/subscribers/{sid}")).status_code == 404
    assert (await pubsub.delete(f"/subscriptions/{sub_id}")).status_code == 404  # cascaded


# S2 ------------------------------------------------------------------------------------------
async def test_s2_subscription_validation(pubsub: httpx.AsyncClient, sdk: DBPersistClient) -> None:
    base = f"/subscribers/{sdk.subscriber_id}/subscriptions"
    assert (await pubsub.post(base, json={"type": "T"})).status_code == 422
    assert (await pubsub.post(base, json={"type": "T", "keys": []})).status_code == 422
    assert (
        await pubsub.post(base, json={"type": "T", "filter": {"field": "q", "op": "gt"}})
    ).status_code == 422
    body = {"type": "T", "filter": {"field": "q", "op": "gt", "value": 1}, "keys": ["a"]}
    assert (await pubsub.post(base, json=body)).status_code == 422
    r = await pubsub.post(base, json={"type": "T", "keys": ["a"], "by": "weird"})
    assert r.status_code == 422
    r = await pubsub.post(
        base, json={"type": "T", "filter": {"field": "q", "op": "gt", "value": 1}}
    )
    assert r.status_code == 201
    assert (await pubsub.delete(f"/subscriptions/{r.json()['id']}")).status_code == 204
    assert (
        await pubsub.post(
            f"/subscribers/{uuid.uuid4()}/subscriptions", json={"type": "T", "keys": ["a"]}
        )
    ).status_code == 404


# S3 ------------------------------------------------------------------------------------------
async def test_s3_filter_subscription_routes_only_matches(
    sdk: DBPersistClient, trade_type: str
) -> None:
    cb = Collector()
    handle = await sdk.subscribe(
        trade_type, {"field": "qty", "op": "gt", "value": 100}, callbacks=cb
    )
    try:
        await sdk.insert(trade_type, {"trade_id": "HIT", "qty": 150})
        await sdk.insert(trade_type, {"trade_id": "MISS", "qty": 5})
        await cb.wait_for(lambda: "HIT" in cb.keys("insert"), "insert of HIT")
        await cb.expect_none("insert", "MISS")
        kind, rec, _ = cb.of("insert")[0]
        assert rec.type_name == trade_type and rec.version == 1 and rec.data["qty"] == 150
        assert cb.errors == []
    finally:
        await sdk.unsubscribe(handle)


# S4 ------------------------------------------------------------------------------------------
async def test_s4_key_and_id_subscriptions(sdk: DBPersistClient, trade_type: str) -> None:
    by_key, by_id = Collector(), Collector()
    target = await sdk.insert(trade_type, {"trade_id": "K1", "qty": 1})
    h1 = await sdk.subscribe(trade_type, keys=["K1"], callbacks=by_key)
    h2 = await sdk.subscribe(trade_type, keys=[str(target.id)], by="id", callbacks=by_id)
    try:
        await sdk.update(trade_type, target.id, 1, {"qty": 2}, mode="merge")
        await sdk.insert(trade_type, {"trade_id": "K2", "qty": 1})  # neither should see this
        await by_key.wait_for(lambda: "K1" in by_key.keys("update"), "key-subscription update")
        await by_id.wait_for(lambda: "K1" in by_id.keys("update"), "id-subscription update")
        await by_key.expect_none("insert", "K2")
        await by_id.expect_none("insert", "K2")
    finally:
        await sdk.unsubscribe(h1)
        await sdk.unsubscribe(h2)


# S5 ------------------------------------------------------------------------------------------
async def test_s5_one_delivery_per_subscriber(
    sdk: DBPersistClient, trade_type: str, pubsub: httpx.AsyncClient
) -> None:
    cb = Collector()
    h1 = await sdk.subscribe(trade_type, {"field": "qty", "op": "gt", "value": 0}, callbacks=cb)
    h2 = await sdk.subscribe(trade_type, keys=["D1"], callbacks=cb)
    try:
        await sdk.insert(trade_type, {"trade_id": "D1", "qty": 1})
        await cb.wait_for(lambda: "D1" in cb.keys("insert"), "insert of D1")
        await asyncio.sleep(1.0)
        assert cb.keys("insert").count("D1") == 1  # two matching subscriptions, one callback
        info = await _client_info(pubsub, sdk.subscriber_id)  # type: ignore[arg-type]
        assert info["pending"] == 0 and info["failed"] == 0
    finally:
        await sdk.unsubscribe(h1)
        await sdk.unsubscribe(h2)


# S6 ------------------------------------------------------------------------------------------
async def test_s6_update_and_delete_payloads(sdk: DBPersistClient, trade_type: str) -> None:
    cb = Collector()
    handle = await sdk.subscribe(trade_type, keys=["U1"], callbacks=cb)
    try:
        rec = await sdk.insert(trade_type, {"trade_id": "U1", "qty": 1, "book": "A"})
        await sdk.update(trade_type, rec.id, 1, {"qty": 2}, mode="merge")
        await sdk.update(
            trade_type, rec.id, 2, {"trade_id": "U1", "qty": 2}, mode="replace"
        )  # drops book
        await sdk.delete(trade_type, rec.id, 3)
        await cb.wait_for(
            lambda: len(cb.of("delete")) == 1 and len(cb.of("update")) == 2,
            "update, update, delete",
        )
        (_, u1, c1), (_, u2, c2) = sorted(cb.of("update"), key=lambda e: e[1].version)
        assert (u1.version, c1) == (2, ["qty"])
        assert (u2.version, c2) == (3, ["book"])
        _, d, _ = cb.of("delete")[0]
        assert d.version == 4 and d.data == {
            "trade_id": "U1",
            "qty": 2,
        }  # last image travels with delete
        assert cb.keys("insert") == ["U1"]
    finally:
        await sdk.unsubscribe(handle)


# S7 ------------------------------------------------------------------------------------------
async def test_s7_transaction_events(
    sdk: DBPersistClient, trade_type: str, pubsub: httpx.AsyncClient
) -> None:
    cb = Collector()
    handle = await sdk.subscribe(trade_type, {"field": "qty", "op": "ge", "value": 0}, callbacks=cb)
    try:
        async with sdk.transaction() as tx:
            tx.insert(trade_type, {"trade_id": "X1", "qty": 1})
            tx.insert(trade_type, {"trade_id": "X2", "qty": 2})
            tx.insert(trade_type, {"trade_id": "X3", "qty": 3})
        await cb.wait_for(
            lambda: set(cb.keys("insert")) >= {"X1", "X2", "X3"}, "three transaction inserts"
        )
        assert tx.result is not None
        # the failed transaction below must not emit anything
        with pytest.raises(DuplicateKey):
            await sdk.commit(
                [
                    {"op": "insert", "type": trade_type, "data": {"trade_id": "X4", "qty": 4}},
                    {"op": "insert", "type": trade_type, "data": {"trade_id": "X1", "qty": 1}},
                ]
            )
        await cb.expect_none("insert", "X4")
    finally:
        await sdk.unsubscribe(handle)


# S8 ------------------------------------------------------------------------------------------
async def test_s8_retry_until_webhook_recovers(
    flaky_sdk: DBPersistClient,
    sdk: DBPersistClient,
    trade_type: str,
    receiver: Receiver,
    pubsub: httpx.AsyncClient,
) -> None:
    cb = Collector()
    handle = await flaky_sdk.subscribe(trade_type, keys=["R1"], callbacks=cb)
    receiver.flaky_failures = 2
    hits_before = receiver.flaky_hits
    try:
        await sdk.insert(trade_type, {"trade_id": "R1", "qty": 1})
        await cb.wait_for(lambda: "R1" in cb.keys("insert"), "delivery after retries", timeout=60)
        assert receiver.flaky_hits - hits_before == 3  # 2 induced failures + 1 success
        await asyncio.sleep(0.5)
        info = await _client_info(pubsub, flaky_sdk.subscriber_id)  # type: ignore[arg-type]
        assert info["pending"] == 0 and info["failed"] == 0 and info["last_delivery_ok_at"]
    finally:
        receiver.flaky_failures = 0
        await flaky_sdk.unsubscribe(handle)


# S9 ------------------------------------------------------------------------------------------
async def test_s9_admin_views(
    sdk: DBPersistClient, trade_type: str, pubsub: httpx.AsyncClient
) -> None:
    before = (await pubsub.get("/admin/stats")).json()
    cb = Collector()
    flt = {"field": "qty", "op": "gt", "value": 1}
    handle = await sdk.subscribe(trade_type, flt, callbacks=cb)
    try:
        r = await pubsub.get(f"/admin/clients/{sdk.subscriber_id}/criteria")
        assert any(s["id"] == str(handle.id) and s["filter"] == flt for s in r.json())
        info = await _client_info(pubsub, sdk.subscriber_id)  # type: ignore[arg-type]
        assert info["subscriptions"] >= 1 and info["name"] == sdk.subscriber_name

        await sdk.insert(trade_type, {"trade_id": "A1", "qty": 5})
        await cb.wait_for(lambda: "A1" in cb.keys("insert"), "insert of A1")
        # the worker bumps "delivered" after our webhook returns, so poll briefly
        for _ in range(40):
            after = (await pubsub.get("/admin/stats")).json()
            if after["delivered"] > before["delivered"]:
                break
            await asyncio.sleep(0.25)
        assert after["received"] > before["received"]
        assert after["matched"] > before["matched"]
        assert after["delivered"] > before["delivered"]

        r = await pubsub.post("/admin/remove-idle", params={"idle_minutes": 60})
        assert sdk.subscriber_name not in r.json()["removed"]  # active subscriber survives
        assert (await pubsub.put("/admin/log-level", json={"level": "DEBUG"})).status_code == 200
        assert (await pubsub.put("/admin/log-level", json={"level": "INFO"})).status_code == 200
    finally:
        await sdk.unsubscribe(handle)


# S10 -----------------------------------------------------------------------------------------
async def test_s10_unsubscribe_stops_delivery(sdk: DBPersistClient, trade_type: str) -> None:
    cb = Collector()
    handle = await sdk.subscribe(trade_type, keys=["Z1"], callbacks=cb)
    rec = await sdk.insert(trade_type, {"trade_id": "Z1", "qty": 1})
    await cb.wait_for(lambda: "Z1" in cb.keys("insert"), "insert of Z1")
    await sdk.unsubscribe(handle)
    await sdk.update(trade_type, rec.id, 1, {"qty": 2}, mode="merge")
    await cb.expect_none("update", "Z1")


# S11 -----------------------------------------------------------------------------------------
async def test_s11_publish_only_reaches_subscribers(sdk: DBPersistClient, trade_type: str) -> None:
    cb = Collector()
    handle = await sdk.subscribe(
        trade_type, {"field": "qty", "op": "eq", "value": 42}, callbacks=cb
    )
    try:
        await sdk.publish_only(
            trade_type, "update", {"trade_id": "V1", "qty": 42}, changed_fields=["qty"]
        )
        await cb.wait_for(lambda: "V1" in cb.keys("update"), "publish-only update")
        _, rec, changed = cb.of("update")[0]
        assert rec.id is None and changed == ["qty"]  # nothing persisted, so no object id
        assert await sdk.count(trade_type) == 0
    finally:
        await sdk.unsubscribe(handle)


# S12 -----------------------------------------------------------------------------------------
async def test_s12_active_select_sees_existing_and_new(
    sdk: DBPersistClient, trade_type: str
) -> None:
    await sdk.insert(trade_type, {"trade_id": "E1", "qty": 10})
    cb = Collector()
    rows, handle = await sdk.active_select(
        trade_type, {"field": "qty", "op": "ge", "value": 10}, cb
    )
    try:
        assert [r.key for r in rows] == ["E1"]
        await sdk.insert(trade_type, {"trade_id": "E2", "qty": 20})
        await cb.wait_for(lambda: "E2" in cb.keys("insert"), "insert of E2")
    finally:
        await sdk.unsubscribe(handle)
