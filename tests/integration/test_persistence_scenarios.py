"""Persistence scenarios P1-P13 (see docs/TEST_CASES.md) against the live service."""

import asyncio
import uuid

import httpx
import pytest
import pytest_asyncio

from dbpersist_sdk import DBPersistClient, DuplicateKey, InvalidData, NotFound, VersionConflict

from .conftest import PERSIST_URL, unique

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# P1 ------------------------------------------------------------------------------------------
async def test_p1_type_registry(sdk: DBPersistClient, persist: httpx.AsyncClient) -> None:
    name = unique("P1")
    info = await sdk.register_type(name, ["a", "b"], {"type": "object"})
    assert info.key_fields == ["a", "b"]
    r = await persist.post("/types", json={"name": name, "key_fields": ["a"]})
    assert r.status_code == 409
    r = await persist.post("/types", json={"name": "bad name", "key_fields": ["a"]})
    assert r.status_code == 422
    r = await persist.post("/types", json={"name": unique("P1"), "key_fields": []})
    assert r.status_code == 422
    assert (await sdk.get_type(name)).name == name
    with pytest.raises(NotFound):
        await sdk.get_type(unique("Missing"))


# P2 ------------------------------------------------------------------------------------------
async def test_p2_insert_and_read(sdk: DBPersistClient, trade_type: str) -> None:
    rec = await sdk.insert(trade_type, {"trade_id": "T1", "qty": 10, "book": "A"})
    assert rec.version == 1 and rec.key == "T1" and rec.id is not None
    assert (await sdk.get(trade_type, rec.id)).data["qty"] == 10
    assert (await sdk.get_by_key(trade_type, "T1")).id == rec.id
    with pytest.raises(DuplicateKey):
        await sdk.insert(trade_type, {"trade_id": "T1", "qty": 99})
    with pytest.raises(NotFound):
        await sdk.get(trade_type, uuid.uuid4())
    with pytest.raises(NotFound):
        await sdk.get_by_key(trade_type, "nope")


async def test_p2b_composite_key(sdk: DBPersistClient) -> None:
    name = unique("Pos")
    await sdk.register_type(name, ["book", "symbol"])
    rec = await sdk.insert(name, {"book": "B1", "symbol": "IBM", "qty": 5})
    assert rec.key == "B1\x1fIBM"
    assert (await sdk.get_by_key(name, "B1", "IBM")).id == rec.id
    with pytest.raises(DuplicateKey):
        await sdk.insert(name, {"book": "B1", "symbol": "IBM"})
    await sdk.insert(name, {"book": "B2", "symbol": "IBM"})  # different composite key is fine


# P3 ------------------------------------------------------------------------------------------
async def test_p3_schema_and_key_validation(sdk: DBPersistClient, trade_type: str) -> None:
    with pytest.raises(InvalidData):
        await sdk.insert(trade_type, {"trade_id": "T1"})  # missing required qty
    with pytest.raises(InvalidData):
        await sdk.insert(trade_type, {"trade_id": "T1", "qty": "lots"})  # wrong type
    with pytest.raises(InvalidData):
        await sdk.insert(trade_type, {"trade_id": None, "qty": 1})  # null key field


# P4 ------------------------------------------------------------------------------------------
async def test_p4_replace_and_merge(sdk: DBPersistClient, trade_type: str) -> None:
    rec = await sdk.insert(
        trade_type, {"trade_id": "T1", "qty": 10, "book": "A", "leg": {"a": 1, "b": 2}}
    )
    merged = await sdk.update(trade_type, rec.id, 1, {"qty": 20, "leg": {"b": 3}}, mode="merge")
    assert merged.version == 2
    assert merged.data == {"trade_id": "T1", "qty": 20, "book": "A", "leg": {"a": 1, "b": 3}}
    replaced = await sdk.update(
        trade_type, rec.id, 2, {"trade_id": "T1", "qty": 30}, mode="replace"
    )
    assert replaced.version == 3 and replaced.data == {"trade_id": "T1", "qty": 30}
    with pytest.raises(InvalidData):  # key fields are immutable
        await sdk.update(trade_type, rec.id, 3, {"trade_id": "OTHER", "qty": 1})


# P5 ------------------------------------------------------------------------------------------
async def test_p5_optimistic_locking(sdk: DBPersistClient, trade_type: str) -> None:
    rec = await sdk.insert(trade_type, {"trade_id": "T1", "qty": 1})
    await sdk.update(trade_type, rec.id, 1, {"qty": 2}, mode="merge")
    with pytest.raises(VersionConflict) as ei:
        await sdk.update(trade_type, rec.id, 1, {"qty": 3}, mode="merge")
    assert ei.value.current_version == 2
    with pytest.raises(VersionConflict):
        await sdk.delete(trade_type, rec.id, 1)
    assert (await sdk.get(trade_type, rec.id)).data["qty"] == 2


# P6 ------------------------------------------------------------------------------------------
async def test_p6_soft_delete_and_key_reuse(sdk: DBPersistClient, trade_type: str) -> None:
    rec = await sdk.insert(trade_type, {"trade_id": "T1", "qty": 1})
    await sdk.delete(trade_type, rec.id, 1)
    with pytest.raises(NotFound):
        await sdk.get(trade_type, rec.id)
    assert await sdk.count(trade_type) == 0
    again = await sdk.insert(trade_type, {"trade_id": "T1", "qty": 2})
    assert again.id != rec.id and again.version == 1


# P7 ------------------------------------------------------------------------------------------
ROWS = [
    {"trade_id": "T1", "qty": 150, "book": "ALPHA", "leg": {"n": 1}},
    {"trade_id": "T2", "qty": 50, "book": "BETA", "leg": {"n": 2}},
    {"trade_id": "T3", "qty": 150.5, "book": None},
    {"trade_id": "T4", "qty": 0, "book": "ALPINE", "flag": True},
    {
        "trade_id": "T5",
        "qty": -3,
        "book": "alpha",
        "flag": False,
        "when": "2024-06-01T00:00:00+00:00",
    },
]

CASES = [
    ({"field": "qty", "op": "gt", "value": 100}, {"T1", "T3"}),
    ({"field": "qty", "op": "le", "value": 0}, {"T4", "T5"}),
    ({"field": "qty", "op": "eq", "value": 150}, {"T1"}),
    ({"field": "qty", "op": "in", "value": [0, 50]}, {"T2", "T4"}),
    ({"field": "book", "op": "eq", "value": "ALPHA"}, {"T1"}),
    ({"field": "book", "op": "like", "value": "ALP%"}, {"T1", "T4"}),
    ({"field": "book", "op": "not_like", "value": "ALP%"}, {"T2", "T5"}),
    ({"field": "book", "op": "is_null"}, {"T3"}),
    ({"field": "book", "op": "is_not_null"}, {"T1", "T2", "T4", "T5"}),
    ({"field": "flag", "op": "eq", "value": True}, {"T4"}),
    ({"field": "leg.n", "op": "eq", "value": 2}, {"T2"}),
    ({"field": "leg.n", "op": "is_null"}, {"T3", "T4", "T5"}),
    (
        {
            "field": "when",
            "op": "lt",
            "value": "2025-01-01T00:00:00+00:00",
            "value_type": "datetime",
        },
        {"T5"},
    ),
    (
        {
            "and": [
                {"field": "qty", "op": "gt", "value": 0},
                {"field": "book", "op": "like", "value": "%A"},
            ]
        },
        {"T1", "T2"},
    ),
    (
        {"or": [{"field": "qty", "op": "lt", "value": 0}, {"field": "book", "op": "is_null"}]},
        {"T3", "T5"},
    ),
    ({"field": "key", "op": "in", "value": ["T1", "T5"]}, {"T1", "T5"}),
    ({"field": "version", "op": "eq", "value": 1}, {"T1", "T2", "T3", "T4", "T5"}),
]


@pytest_asyncio.fixture(loop_scope="session")
async def seeded(sdk: DBPersistClient) -> str:
    name = unique("P7")
    await sdk.register_type(name, ["trade_id"])
    for row in ROWS:
        await sdk.insert(name, row)
    return name


@pytest.mark.parametrize(("flt", "expected"), CASES, ids=[str(c[0]) for c in CASES])
async def test_p7_criteria_queries(
    sdk: DBPersistClient, seeded: str, flt: dict, expected: set[str]
) -> None:
    rows = await sdk.select(seeded, flt)
    assert {r.key for r in rows} == expected
    assert await sdk.count(seeded, flt) == len(expected)


async def test_p7b_invalid_filter_rejected(sdk: DBPersistClient, seeded: str) -> None:
    with pytest.raises(InvalidData):
        await sdk.select(seeded, {"field": "qty", "op": "gt"})
    with pytest.raises(InvalidData):
        await sdk.select(seeded, {"field": "bad-name", "op": "eq", "value": 1})


# P8 ------------------------------------------------------------------------------------------
async def test_p8_ordering_offset_and_cursor(
    sdk: DBPersistClient, seeded: str, persist: httpx.AsyncClient
) -> None:
    rows = await sdk.select(seeded, order_by=[{"field": "key", "desc": True}], limit=2)
    assert [r.key for r in rows] == ["T5", "T4"]
    rows = await sdk.select(seeded, order_by=[{"field": "key"}], limit=2, offset=3)
    assert [r.key for r in rows] == ["T4", "T5"]
    rows = await sdk.select(seeded, order_by=[{"field": "qty"}])
    assert [r.key for r in rows] == ["T5", "T4", "T2", "T1", "T3"]

    streamed = [r.key async for r in sdk.select_iter(seeded, page_size=2)]
    assert sorted(streamed) == ["T1", "T2", "T3", "T4", "T5"] and len(streamed) == 5

    r = await persist.post(f"/objects/{seeded}/query", json={"cursor": "garbage"})
    assert r.status_code == 422
    r = await persist.post(f"/objects/{seeded}/query", json={"limit": 0})
    assert r.status_code == 422


# P9 ------------------------------------------------------------------------------------------
async def test_p9_transactions(sdk: DBPersistClient, trade_type: str, account_type_of: str) -> None:
    base = await sdk.insert(trade_type, {"trade_id": "T0", "qty": 1})

    async with sdk.transaction() as tx:
        tx.insert(trade_type, {"trade_id": "T1", "qty": 10})
        tx.update(trade_type, base.id, 1, {"qty": 2}, mode="merge")
        async with tx.begin():  # nested scope: still one commit
            tx.insert(account_type_of, {"acct": "A1"})
    assert tx.result is not None and len(tx.result.results) == 3
    assert tx.result.results[1].object.version == 2
    assert await sdk.count(trade_type) == 2

    # atomic failure: duplicate key in the second op rolls back the first
    with pytest.raises(DuplicateKey):
        await sdk.commit(
            [
                {"op": "insert", "type": trade_type, "data": {"trade_id": "T2", "qty": 1}},
                {"op": "insert", "type": trade_type, "data": {"trade_id": "T1", "qty": 1}},
            ]
        )
    assert await sdk.count(trade_type) == 2

    # version conflict rolls back too
    with pytest.raises(VersionConflict):
        await sdk.commit(
            [
                {"op": "insert", "type": trade_type, "data": {"trade_id": "T3", "qty": 1}},
                {"op": "delete", "type": trade_type, "id": str(base.id), "version": 99},
            ]
        )
    assert await sdk.count(trade_type) == 2

    # exception inside the context discards the ops without committing
    with pytest.raises(RuntimeError):
        async with sdk.transaction() as tx2:
            tx2.insert(trade_type, {"trade_id": "T9", "qty": 1})
            raise RuntimeError("abort")
    assert await sdk.count(trade_type) == 2


# P10 -----------------------------------------------------------------------------------------
async def test_p10_expand_reference(
    sdk: DBPersistClient, trade_type: str, account_type_of: str
) -> None:
    acct = await sdk.insert(account_type_of, {"acct": "ACC1", "owner": "pat"})
    rec = await sdk.insert(trade_type, {"trade_id": "T1", "qty": 1, "account": str(acct.id)})
    got = await sdk.get(trade_type, rec.id, expand=["account"])
    assert got.data["_expanded"]["account"]["data"]["owner"] == "pat"
    rows = await sdk.select(trade_type, expand=["account"])
    assert rows[0].data["_expanded"]["account"]["id"] == str(acct.id)
    with pytest.raises(InvalidData):
        await sdk.get(trade_type, rec.id, expand=["book"])  # not an x-ref field


# P11 -----------------------------------------------------------------------------------------
async def test_p11_publish_only_writes_nothing(sdk: DBPersistClient, trade_type: str) -> None:
    event_id = await sdk.publish_only(
        trade_type, "update", {"trade_id": "T1", "qty": 5}, changed_fields=["qty"]
    )
    assert isinstance(event_id, uuid.UUID)
    assert await sdk.count(trade_type) == 0
    with pytest.raises(InvalidData):
        await sdk.publish_only(trade_type, "insert", {"qty": 5})  # key field missing


# P12 -----------------------------------------------------------------------------------------
async def test_p12_api_key_required() -> None:
    async with httpx.AsyncClient(base_url=f"{PERSIST_URL}/v1", timeout=10) as c:
        assert (await c.get("/types")).status_code == 401
        assert (await c.get("/types", headers={"X-API-Key": "wrong"})).status_code == 401
    async with httpx.AsyncClient(base_url=PERSIST_URL, timeout=10) as c:
        assert (await c.get("/health")).status_code == 200  # health is open


# P13 -----------------------------------------------------------------------------------------
async def test_p13_concurrent_updates_serialise(sdk: DBPersistClient, trade_type: str) -> None:
    rec = await sdk.insert(trade_type, {"trade_id": "T1", "qty": 0})
    conflicts = 0

    async def bump(i: int) -> None:
        nonlocal conflicts
        while True:
            cur = await sdk.get(trade_type, rec.id)
            try:
                await sdk.update(
                    trade_type,
                    rec.id,
                    cur.version,
                    {"qty": cur.data["qty"] + 1, f"w{i}": True},
                    mode="merge",
                )
                return
            except VersionConflict:
                conflicts += 1

    await asyncio.gather(*(bump(i) for i in range(10)))
    final = await sdk.get(trade_type, rec.id)
    assert final.version == 11 and final.data["qty"] == 10
    assert all(final.data.get(f"w{i}") for i in range(10))
    assert conflicts > 0  # the race really happened and was detected
