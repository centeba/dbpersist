"""Scoped API keys: a key only touches the types its scopes cover."""

from httpx import AsyncClient

from dbpersist_persistence.services.events import RecordingPublisher

ORDERS_RW = [{"type_prefix": "orders.", "permissions": ["read", "write"]}]


async def mint(client: AsyncClient, name: str, scopes: list[dict]) -> tuple[str, str]:
    """Create a key with the bootstrap admin; returns (key, id)."""
    r = await client.post("/v1/api-keys", json={"name": name, "scopes": scopes})
    assert r.status_code == 201, r.text
    return r.json()["key"], r.json()["id"]


async def two_domains(client: AsyncClient) -> None:
    for name in ("orders.Order", "billing.Invoice"):
        r = await client.post("/v1/types", json={"name": name, "key_fields": ["k"]})
        assert r.status_code == 201, r.text


async def test_scoped_key_stays_in_its_domain(client: AsyncClient) -> None:
    await two_domains(client)
    key, _ = await mint(client, "orders-svc", ORDERS_RW)
    h = {"X-API-Key": key}

    r = await client.post("/v1/objects/orders.Order", json={"k": "a"}, headers=h)
    assert r.status_code == 201, r.text
    oid = r.json()["id"]
    assert (await client.get(f"/v1/objects/orders.Order/{oid}", headers=h)).status_code == 200
    r = await client.post("/v1/objects/orders.Order/query", json={}, headers=h)
    assert r.status_code == 200 and len(r.json()["items"]) == 1

    r = await client.post("/v1/objects/billing.Invoice", json={"k": "a"}, headers=h)
    assert r.status_code == 403
    assert "billing.Invoice" in r.json()["detail"]
    assert (
        await client.post("/v1/objects/billing.Invoice/query", json={}, headers=h)
    ).status_code == 403
    assert (
        await client.post("/v1/objects/billing.Invoice/count", json={}, headers=h)
    ).status_code == 403
    r = await client.post(
        "/v1/publish",
        json={"type": "billing.Invoice", "op": "update", "data": {"k": "a"}},
        headers=h,
    )
    assert r.status_code == 403

    # the type registry is filtered to what the key may read; registering needs admin
    r = await client.get("/v1/types", headers=h)
    assert [t["name"] for t in r.json()] == ["orders.Order"]
    assert (await client.get("/v1/types/billing.Invoice", headers=h)).status_code == 403
    r = await client.post("/v1/types", json={"name": "orders.Fill", "key_fields": ["k"]}, headers=h)
    assert r.status_code == 403

    # operator surfaces need a platform admin
    assert (await client.get("/v1/inbound-sources", headers=h)).status_code == 403
    assert (await client.get("/v1/api-keys", headers=h)).status_code == 403
    r = await client.post("/v1/api-keys", json={"name": "x", "scopes": ORDERS_RW}, headers=h)
    assert r.status_code == 403


async def test_transaction_with_one_out_of_scope_op_does_nothing(
    client: AsyncClient, publisher: RecordingPublisher
) -> None:
    await two_domains(client)
    key, _ = await mint(client, "orders-svc", ORDERS_RW)
    r = await client.post(
        "/v1/transactions",
        json={
            "ops": [
                {"op": "insert", "type": "orders.Order", "data": {"k": "t1"}},
                {"op": "insert", "type": "billing.Invoice", "data": {"k": "t1"}},
            ]
        },
        headers={"X-API-Key": key},
    )
    assert r.status_code == 403
    r = await client.post("/v1/objects/orders.Order/count", json={})
    assert r.json()["count"] == 0
    assert publisher.events == []


async def test_domain_admin_registers_types_under_its_prefix(client: AsyncClient) -> None:
    key, _ = await mint(
        client, "orders-admin", [{"type_prefix": "orders.", "permissions": ["admin"]}]
    )
    h = {"X-API-Key": key}
    r = await client.post("/v1/types", json={"name": "orders.Fill", "key_fields": ["k"]}, headers=h)
    assert r.status_code == 201
    r = await client.post(
        "/v1/types", json={"name": "billing.Invoice", "key_fields": ["k"]}, headers=h
    )
    assert r.status_code == 403
    # admin implies write and read on the same prefix
    assert (
        await client.post("/v1/objects/orders.Fill", json={"k": "f"}, headers=h)
    ).status_code == 201
    # but not platform admin
    assert (await client.get("/v1/api-keys", headers=h)).status_code == 403


async def test_read_only_key_cannot_write(client: AsyncClient) -> None:
    await two_domains(client)
    key, _ = await mint(client, "reporting", [{"type_prefix": "*", "permissions": ["read"]}])
    h = {"X-API-Key": key}
    await client.post("/v1/objects/orders.Order", json={"k": "a", "v": 1})
    r = await client.post("/v1/objects/orders.Order/query", json={}, headers=h)
    assert r.status_code == 200 and len(r.json()["items"]) == 1
    oid = r.json()["items"][0]["id"]
    assert (
        await client.post("/v1/objects/billing.Invoice", json={"k": "b"}, headers=h)
    ).status_code == 403
    r = await client.patch(
        f"/v1/objects/orders.Order/{oid}", json={"version": 1, "data": {"v": 2}}, headers=h
    )
    assert r.status_code == 403
    assert (
        await client.delete(f"/v1/objects/orders.Order/{oid}?version=1", headers=h)
    ).status_code == 403


async def test_expand_needs_read_on_the_referenced_type(
    client: AsyncClient, trade_type: dict
) -> None:
    acct = await client.post("/v1/objects/Account", json={"acct": "A1"})
    await client.post(
        "/v1/objects/Trade", json={"trade_id": "T1", "qty": 1, "account": acct.json()["id"]}
    )
    key, _ = await mint(client, "trades-only", [{"type_prefix": "Trade", "permissions": ["read"]}])
    h = {"X-API-Key": key}
    assert (await client.get("/v1/objects/Trade/by-key/T1", headers=h)).status_code == 200
    r = await client.get("/v1/objects/Trade/by-key/T1?expand=account", headers=h)
    assert r.status_code == 403
    assert "Account" in r.json()["detail"]


async def test_key_lifecycle(client: AsyncClient) -> None:
    await two_domains(client)
    key, kid = await mint(client, "orders-svc", ORDERS_RW)
    h = {"X-API-Key": key}
    ok = "/v1/objects/orders.Order/count"
    assert (await client.post(ok, json={}, headers=h)).status_code == 200

    # listing shows a hint, never the secret
    r = await client.get("/v1/api-keys")
    [row] = r.json()
    assert row["name"] == "orders-svc" and row["key_hint"] == key[-4:] and "key" not in row
    assert row["last_used_at"] is not None

    # duplicate name is rejected
    r = await client.post("/v1/api-keys", json={"name": "orders-svc", "scopes": ORDERS_RW})
    assert r.status_code == 409

    # disable takes effect at once, enable restores, delete is final
    assert (await client.post(f"/v1/api-keys/{kid}/disable")).status_code == 200
    assert (await client.post(ok, json={}, headers=h)).status_code == 401
    assert (await client.post(f"/v1/api-keys/{kid}/enable")).status_code == 200
    assert (await client.post(ok, json={}, headers=h)).status_code == 200
    assert (await client.delete(f"/v1/api-keys/{kid}")).status_code == 204
    assert (await client.post(ok, json={}, headers=h)).status_code == 401

    # unknown and missing keys
    assert (await client.post(ok, json={}, headers={"X-API-Key": "dbp_nope"})).status_code == 401
    assert (await client.post(ok, json={}, headers={"X-API-Key": ""})).status_code == 401


async def test_registering_an_existing_secret(client: AsyncClient) -> None:
    """The SDK registers one secret in both services: the second service receives it as-is."""
    secret = "dbp_" + "x" * 40
    r = await client.post(
        "/v1/api-keys", json={"name": "shared", "scopes": ORDERS_RW, "key": secret}
    )
    assert r.status_code == 201 and r.json()["key"] == secret
    r = await client.post(
        "/v1/api-keys", json={"name": "shared-2", "scopes": ORDERS_RW, "key": secret}
    )
    assert r.status_code == 409  # same secret twice
    r = await client.post("/v1/api-keys", json={"name": "short", "scopes": ORDERS_RW, "key": "abc"})
    assert r.status_code == 422


async def test_scope_validation(client: AsyncClient) -> None:
    bad = [
        {"name": "a", "scopes": []},
        {"name": "a", "scopes": [{"type_prefix": "orders.", "permissions": []}]},
        {"name": "a", "scopes": [{"type_prefix": "orders.", "permissions": ["delete"]}]},
        {"name": "a", "scopes": [{"type_prefix": "has space", "permissions": ["read"]}]},
    ]
    for body in bad:
        assert (await client.post("/v1/api-keys", json=body)).status_code == 422, body
