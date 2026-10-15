from httpx import AsyncClient

from dbpersist_persistence.services.events import RecordingPublisher


async def test_transaction_commits_atomically(
    client: AsyncClient, trade_type: dict, publisher: RecordingPublisher
) -> None:
    first = (await client.post("/v1/objects/Trade", json={"trade_id": "T0", "qty": 1})).json()
    publisher.events.clear()

    body = {
        "ops": [
            {"op": "insert", "type": "Trade", "data": {"trade_id": "T1", "qty": 10}},
            {
                "op": "update",
                "type": "Trade",
                "id": first["id"],
                "version": 1,
                "data": {"qty": 2},
                "mode": "merge",
            },
            {"op": "insert", "type": "Account", "data": {"acct": "A1"}},
        ]
    }
    r = await client.post("/v1/transactions", json=body)
    assert r.status_code == 200, r.text
    out = r.json()
    assert [x["op"] for x in out["results"]] == ["insert", "update", "insert"]
    assert out["results"][1]["object"]["version"] == 2
    assert len(publisher.events) == 3
    assert {e.transaction_id for e in publisher.events} == {
        __import__("uuid").UUID(out["transaction_id"])
    }
    assert publisher.events[1].changed_fields == ["qty"]

    r = await client.delete(f"/v1/objects/Trade/{first['id']}", params={"version": 2})
    assert r.status_code == 204


async def test_transaction_rolls_back_on_failure(
    client: AsyncClient, trade_type: dict, publisher: RecordingPublisher
) -> None:
    body = {
        "ops": [
            {"op": "insert", "type": "Trade", "data": {"trade_id": "T1", "qty": 10}},
            {"op": "insert", "type": "Trade", "data": {"trade_id": "T1", "qty": 11}},
        ]
    }
    r = await client.post("/v1/transactions", json=body)
    assert r.status_code == 409
    assert publisher.events == []
    r = await client.post("/v1/objects/Trade/count", json={})
    assert r.json()["count"] == 0


async def test_transaction_version_conflict(
    client: AsyncClient, trade_type: dict, publisher: RecordingPublisher
) -> None:
    first = (await client.post("/v1/objects/Trade", json={"trade_id": "T0", "qty": 1})).json()
    publisher.events.clear()
    body = {"ops": [{"op": "delete", "type": "Trade", "id": first["id"], "version": 7}]}
    r = await client.post("/v1/transactions", json=body)
    assert r.status_code == 409 and r.json()["code"] == "version_conflict"
    assert publisher.events == []


async def test_publish_only(
    client: AsyncClient, trade_type: dict, publisher: RecordingPublisher
) -> None:
    r = await client.post(
        "/v1/publish",
        json={
            "type": "Trade",
            "op": "update",
            "data": {"trade_id": "T1", "qty": 3},
            "changed_fields": ["qty"],
        },
    )
    assert r.status_code == 202, r.text
    assert r.json()["key"] == "T1"
    assert publisher.events[-1].op == "update" and publisher.events[-1].object_id is None
    r = await client.post("/v1/objects/Trade/count", json={})
    assert r.json()["count"] == 0
    r = await client.post("/v1/publish", json={"type": "Trade", "op": "insert", "data": {"qty": 3}})
    assert r.status_code == 422
