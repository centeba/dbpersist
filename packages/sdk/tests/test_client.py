import json
import uuid

import httpx
import pytest
import respx

from dbpersist_sdk import DBPersistClient, VersionConflict

P = "http://persist.test/v1"
S = "http://pubsub.test/v1"


def rec(key: str = "T1", version: int = 1, **data) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "type_name": "Trade",
        "key": key,
        "version": version,
        "data": {"trade_id": key, **data},
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    }


@pytest.fixture
async def client():
    async with DBPersistClient(
        "http://persist.test",
        "http://pubsub.test",
        "k",
        subscriber_name="app",
        webhook_url="http://me/hook",
        cache=True,
    ) as c:
        yield c


@respx.mock
async def test_insert_select_and_cache(client: DBPersistClient) -> None:
    respx.post(f"{P}/objects/Trade").mock(return_value=httpx.Response(201, json=rec(qty=1)))
    r = await client.insert("Trade", {"trade_id": "T1", "qty": 1})
    assert r.key == "T1" and client.cache[("Trade", "T1")] is r

    route = respx.post(f"{P}/objects/Trade/query").mock(
        return_value=httpx.Response(
            200, json={"items": [rec("T1", 2, qty=5), rec("T2", 1, qty=9)], "next_cursor": None}
        )
    )
    rows = await client.select("Trade", {"field": "qty", "op": "gt", "value": 0}, limit=10)
    assert [x.key for x in rows] == ["T1", "T2"]
    sent = json.loads(route.calls.last.request.content)
    assert sent == {"filter": {"field": "qty", "op": "gt", "value": 0}, "limit": 10, "offset": 0}
    assert client.cache[("Trade", "T1")].version == 2


@respx.mock
async def test_select_iter_follows_cursor(client: DBPersistClient) -> None:
    route = respx.post(f"{P}/objects/Trade/query")
    route.side_effect = [
        httpx.Response(200, json={"items": [rec("T1"), rec("T2")], "next_cursor": "abc"}),
        httpx.Response(200, json={"items": [rec("T3")], "next_cursor": None}),
    ]
    keys = [r.key async for r in client.select_iter("Trade", page_size=2)]
    assert keys == ["T1", "T2", "T3"]
    assert json.loads(route.calls[1].request.content)["cursor"] == "abc"


@respx.mock
async def test_errors_map(client: DBPersistClient) -> None:
    obj_id = uuid.uuid4()
    respx.put(f"{P}/objects/Trade/{obj_id}").mock(
        return_value=httpx.Response(
            409, json={"code": "version_conflict", "detail": "x", "current_version": 4}
        )
    )
    with pytest.raises(VersionConflict) as ei:
        await client.update("Trade", obj_id, 1, {"trade_id": "T1"})
    assert ei.value.current_version == 4


@respx.mock
async def test_nested_transaction_commits_once(client: DBPersistClient) -> None:
    route = respx.post(f"{P}/transactions").mock(
        return_value=httpx.Response(
            200,
            json={
                "transaction_id": str(uuid.uuid4()),
                "results": [
                    {"op": "insert", "object": rec("T1")},
                    {"op": "delete", "object": rec("T2")},
                ],
            },
        )
    )
    client.cache[("Trade", "T2")] = client.cache.get(("Trade", "T2")) or __import__(
        "dbpersist_sdk"
    ).ObjectRecord.model_validate(rec("T2"))
    async with client.transaction() as tx:
        tx.insert("Trade", {"trade_id": "T1"})
        async with tx.begin():
            tx.delete("Trade", uuid.uuid4(), 1)
        assert route.call_count == 0
    assert route.call_count == 1
    assert len(json.loads(route.calls.last.request.content)["ops"]) == 2
    assert (
        tx.result is not None
        and ("Trade", "T2") not in client.cache
        and ("Trade", "T1") in client.cache
    )


@respx.mock
async def test_transaction_discarded_on_exception(client: DBPersistClient) -> None:
    route = respx.post(f"{P}/transactions")
    with pytest.raises(RuntimeError):
        async with client.transaction() as tx:
            tx.insert("Trade", {})
            raise RuntimeError("abort")
    assert route.call_count == 0


@respx.mock
async def test_register_subscribe_active_select_order(client: DBPersistClient) -> None:
    sid = uuid.uuid4()
    respx.post(f"{S}/subscribers").mock(
        return_value=httpx.Response(
            201,
            json={"id": str(sid), "name": "app", "webhook_url": "http://me/hook", "secret": "s3"},
        )
    )
    info = await client.register_subscriber()
    assert client.secret == "s3" and client.subscriber_id == sid and info.secret == "s3"

    calls: list[str] = []
    sub_id = uuid.uuid4()

    def sub_handler(request):
        calls.append("subscribe")
        return httpx.Response(
            201,
            json={
                "id": str(sub_id),
                "subscriber_id": str(sid),
                "type_name": "Trade",
                "filter": json.loads(request.content)["filter"],
                "keys": None,
                "by": "key",
                "created_at": "2026-01-01T00:00:00Z",
            },
        )

    def query_handler(request):
        calls.append("select")
        return httpx.Response(200, json={"items": [], "next_cursor": None})

    respx.post(f"{S}/subscribers/{sid}/subscriptions").mock(side_effect=sub_handler)
    respx.post(f"{P}/objects/Trade/query").mock(side_effect=query_handler)

    class CB:
        pass

    cb = CB()
    rows, handle = await client.active_select("Trade", {"field": "qty", "op": "gt", "value": 1}, cb)  # type: ignore[arg-type]
    assert rows == [] and handle.id == sub_id and client.callbacks[sub_id] is cb
    assert calls == ["subscribe", "select"]

    respx.delete(f"{S}/subscriptions/{sub_id}").mock(return_value=httpx.Response(204))
    await client.unsubscribe(handle)
    assert sub_id not in client.callbacks
    respx.post(f"{S}/subscribers/{sid}/heartbeat").mock(return_value=httpx.Response(200, json={}))
    await client.heartbeat()
