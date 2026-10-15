import uuid

from httpx import AsyncClient

from dbpersist_persistence.services.events import RecordingPublisher


async def test_requires_api_key(client: AsyncClient) -> None:
    r = await client.get("/v1/types", headers={"X-API-Key": "wrong"})
    assert r.status_code == 401


async def test_type_registration(client: AsyncClient, trade_type: dict) -> None:
    r = await client.get("/v1/types/Trade")
    assert r.status_code == 200
    assert r.json()["key_fields"] == ["trade_id"]
    r = await client.post("/v1/types", json={"name": "Trade", "key_fields": ["x"]})
    assert r.status_code == 409
    r = await client.get("/v1/types/Nope")
    assert r.status_code == 404


async def test_insert_get_duplicate(
    client: AsyncClient, trade_type: dict, publisher: RecordingPublisher
) -> None:
    r = await client.post("/v1/objects/Trade", json={"trade_id": "T1", "qty": 150, "book": "A"})
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["key"] == "T1" and body["version"] == 1
    obj_id = body["id"]

    r = await client.get(f"/v1/objects/Trade/{obj_id}")
    assert r.status_code == 200 and r.json()["data"]["qty"] == 150
    r = await client.get("/v1/objects/Trade/by-key/T1")
    assert r.status_code == 200 and r.json()["id"] == obj_id

    r = await client.post("/v1/objects/Trade", json={"trade_id": "T1", "qty": 1})
    assert r.status_code == 409 and r.json()["code"] == "duplicate_key"

    assert len(publisher.events) == 1
    ev = publisher.events[0]
    assert ev.op == "insert" and ev.key == "T1" and ev.type_name == "Trade" and ev.version == 1


async def test_schema_validation(client: AsyncClient, trade_type: dict) -> None:
    r = await client.post("/v1/objects/Trade", json={"trade_id": "T1"})
    assert r.status_code == 422 and r.json()["code"] == "invalid_data"
    r = await client.post("/v1/objects/Trade", json={"trade_id": "T1", "qty": "lots"})
    assert r.status_code == 422


async def test_update_replace_merge_and_versioning(
    client: AsyncClient, trade_type: dict, publisher: RecordingPublisher
) -> None:
    r = await client.post("/v1/objects/Trade", json={"trade_id": "T1", "qty": 150, "book": "A"})
    obj_id = r.json()["id"]

    r = await client.patch(f"/v1/objects/Trade/{obj_id}", json={"version": 1, "data": {"qty": 160}})
    assert r.status_code == 200, r.text
    assert r.json()["version"] == 2 and r.json()["data"] == {
        "trade_id": "T1",
        "qty": 160,
        "book": "A",
    }
    assert publisher.events[-1].op == "update" and publisher.events[-1].changed_fields == ["qty"]

    r = await client.patch(f"/v1/objects/Trade/{obj_id}", json={"version": 1, "data": {"qty": 1}})
    assert (
        r.status_code == 409
        and r.json()["code"] == "version_conflict"
        and r.json()["current_version"] == 2
    )

    r = await client.put(
        f"/v1/objects/Trade/{obj_id}", json={"version": 2, "data": {"trade_id": "T1", "qty": 5}}
    )
    assert r.status_code == 200
    assert r.json()["data"] == {"trade_id": "T1", "qty": 5}
    assert sorted(publisher.events[-1].changed_fields) == ["book", "qty"]

    r = await client.put(
        f"/v1/objects/Trade/{obj_id}", json={"version": 3, "data": {"trade_id": "T9", "qty": 5}}
    )
    assert r.status_code == 422


async def test_delete_soft_and_key_reuse(
    client: AsyncClient, trade_type: dict, publisher: RecordingPublisher
) -> None:
    r = await client.post("/v1/objects/Trade", json={"trade_id": "T1", "qty": 1})
    obj_id = r.json()["id"]
    r = await client.delete(f"/v1/objects/Trade/{obj_id}", params={"version": 99})
    assert r.status_code == 409
    r = await client.delete(f"/v1/objects/Trade/{obj_id}", params={"version": 1})
    assert r.status_code == 204
    assert publisher.events[-1].op == "delete" and publisher.events[-1].data == {
        "trade_id": "T1",
        "qty": 1,
    }
    r = await client.get(f"/v1/objects/Trade/{obj_id}")
    assert r.status_code == 404
    r = await client.post("/v1/objects/Trade", json={"trade_id": "T1", "qty": 2})
    assert r.status_code == 201


async def test_expand_reference(client: AsyncClient, trade_type: dict) -> None:
    acct = (await client.post("/v1/objects/Account", json={"acct": "ACC1", "owner": "bob"})).json()
    r = await client.post(
        "/v1/objects/Trade", json={"trade_id": "T1", "qty": 1, "account": acct["id"]}
    )
    obj_id = r.json()["id"]
    r = await client.get(f"/v1/objects/Trade/{obj_id}", params={"expand": "account"})
    assert r.status_code == 200, r.text
    assert r.json()["data"]["_expanded"]["account"]["data"]["owner"] == "bob"
    r = await client.get(f"/v1/objects/Trade/{obj_id}", params={"expand": "book"})
    assert r.status_code == 422


async def test_unknown_object(client: AsyncClient, trade_type: dict) -> None:
    r = await client.get(f"/v1/objects/Trade/{uuid.uuid4()}")
    assert r.status_code == 404


async def test_health_and_readiness(client: AsyncClient) -> None:
    r = await client.get("/health", headers={})
    assert r.status_code == 200 and r.json() == {"status": "ok"}
    r = await client.get("/healthz", headers={})
    assert r.status_code == 200 and r.json() == {"status": "ok", "database": "up"}
