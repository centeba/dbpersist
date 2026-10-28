"""Scoped API keys on the pub/sub side: subscriptions are reads, subscribers have owners."""

from httpx import AsyncClient

EVENT = {
    "type_name": "billing.Invoice",
    "key": "I1",
    "op": "insert",
    "version": 1,
    "data": {"k": "I1"},
    "changed_fields": [],
}


async def mint(client: AsyncClient, name: str, scopes: list[dict]) -> str:
    r = await client.post("/v1/api-keys", json={"name": name, "scopes": scopes})
    assert r.status_code == 201, r.text
    return r.json()["key"]


async def test_subscription_requires_read_on_the_type(client: AsyncClient) -> None:
    key = await mint(client, "orders-svc", [{"type_prefix": "orders.", "permissions": ["read"]}])
    h = {"X-API-Key": key}
    r = await client.post(
        "/v1/subscribers",
        json={"name": "orders-app", "webhook_url": "http://r.test/hook"},
        headers=h,
    )
    assert r.status_code == 201, r.text
    sid = r.json()["id"]
    r = await client.post(
        f"/v1/subscribers/{sid}/subscriptions",
        json={"type": "orders.Order", "keys": ["O1"]},
        headers=h,
    )
    assert r.status_code == 201, r.text
    r = await client.post(
        f"/v1/subscribers/{sid}/subscriptions",
        json={"type": "billing.Invoice", "keys": ["I1"]},
        headers=h,
    )
    assert r.status_code == 403
    assert "billing.Invoice" in r.json()["detail"]


async def test_subscribers_belong_to_the_key_that_created_them(client: AsyncClient) -> None:
    k1 = await mint(client, "svc-1", [{"type_prefix": "orders.", "permissions": ["read"]}])
    k2 = await mint(client, "svc-2", [{"type_prefix": "orders.", "permissions": ["read"]}])
    h1, h2 = {"X-API-Key": k1}, {"X-API-Key": k2}
    r = await client.post(
        "/v1/subscribers", json={"name": "app-1", "webhook_url": "http://r.test/1"}, headers=h1
    )
    sid = r.json()["id"]
    sub = await client.post(
        f"/v1/subscribers/{sid}/subscriptions",
        json={"type": "orders.Order", "keys": ["O1"]},
        headers=h1,
    )
    sub_id = sub.json()["id"]

    # the other key sees nothing of it and cannot touch it
    assert (await client.get("/v1/subscribers", headers=h2)).json() == []
    assert (await client.get(f"/v1/subscribers/{sid}", headers=h2)).status_code == 403
    assert (await client.post(f"/v1/subscribers/{sid}/heartbeat", headers=h2)).status_code == 403
    assert (await client.get(f"/v1/subscribers/{sid}/subscriptions", headers=h2)).status_code == 403
    assert (await client.delete(f"/v1/subscriptions/{sub_id}", headers=h2)).status_code == 403
    assert (await client.delete(f"/v1/subscribers/{sid}", headers=h2)).status_code == 403
    assert (
        await client.post(f"/v1/subscribers/{sid}/replay?since=2026-01-01T00:00:00Z", headers=h2)
    ).status_code == 403

    # the owner and the platform admin can
    assert (await client.post(f"/v1/subscribers/{sid}/heartbeat", headers=h1)).status_code == 200
    assert [s["name"] for s in (await client.get("/v1/subscribers")).json()] == ["app-1"]
    assert (await client.delete(f"/v1/subscriptions/{sub_id}", headers=h1)).status_code == 204
    assert (await client.delete(f"/v1/subscribers/{sid}")).status_code == 204


async def test_publish_needs_write_and_admin_needs_admin(client: AsyncClient) -> None:
    reader = await mint(client, "reader", [{"type_prefix": "billing.", "permissions": ["read"]}])
    writer = await mint(client, "writer", [{"type_prefix": "billing.", "permissions": ["write"]}])
    r = await client.post("/v1/publish", json=EVENT, headers={"X-API-Key": reader})
    assert r.status_code == 403
    r = await client.post("/v1/publish", json=EVENT, headers={"X-API-Key": writer})
    assert r.status_code == 202
    for path in ("/v1/admin/clients", "/v1/admin/stats", "/v1/api-keys"):
        assert (await client.get(path, headers={"X-API-Key": writer})).status_code == 403
    assert (await client.get("/v1/admin/stats")).status_code == 200


async def test_disabled_and_unknown_keys_are_unauthorized(client: AsyncClient) -> None:
    r = await client.post(
        "/v1/api-keys",
        json={"name": "tmp", "scopes": [{"type_prefix": "*", "permissions": ["read"]}]},
    )
    key, kid = r.json()["key"], r.json()["id"]
    h = {"X-API-Key": key}
    assert (await client.get("/v1/subscribers", headers=h)).status_code == 200
    await client.post(f"/v1/api-keys/{kid}/disable")
    assert (await client.get("/v1/subscribers", headers=h)).status_code == 401
    assert (await client.get("/v1/subscribers", headers={"X-API-Key": "nope"})).status_code == 401
