import pytest
from httpx import AsyncClient

from dbpersist_common.filters import matches, parse_filter

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

FILTERS = [
    None,
    {"field": "qty", "op": "gt", "value": 100},
    {"field": "qty", "op": "ge", "value": 150},
    {"field": "qty", "op": "lt", "value": 1},
    {"field": "qty", "op": "eq", "value": 150},
    {"field": "qty", "op": "ne", "value": 150},
    {"field": "qty", "op": "in", "value": [0, 50]},
    {"field": "book", "op": "eq", "value": "ALPHA"},
    {"field": "book", "op": "like", "value": "ALP%"},
    {"field": "book", "op": "not_like", "value": "ALP%"},
    {"field": "book", "op": "in", "value": ["BETA", "alpha"]},
    {"field": "book", "op": "is_null"},
    {"field": "book", "op": "is_not_null"},
    {"field": "flag", "op": "eq", "value": True},
    {"field": "flag", "op": "eq", "value": False},
    {"field": "leg.n", "op": "eq", "value": 2},
    {"field": "leg.n", "op": "is_null"},
    {"field": "when", "op": "lt", "value": "2025-01-01T00:00:00+00:00", "value_type": "datetime"},
    {
        "and": [
            {"field": "qty", "op": "gt", "value": 0},
            {"field": "book", "op": "like", "value": "%A"},
        ]
    },
    {"or": [{"field": "qty", "op": "lt", "value": 0}, {"field": "book", "op": "is_null"}]},
    {"field": "key", "op": "in", "value": ["T1", "T5"]},
    {"field": "version", "op": "eq", "value": 1},
]


@pytest.fixture
async def seeded(client: AsyncClient, trade_type: dict) -> dict[str, dict]:
    # relax schema: just register a loose type for this module
    await client.post("/v1/types", json={"name": "Row", "key_fields": ["trade_id"]})
    out: dict[str, dict] = {}
    for row in ROWS:
        r = await client.post("/v1/objects/Row", json=row)
        assert r.status_code == 201, r.text
        out[r.json()["id"]] = row
    return out


@pytest.mark.parametrize("raw", FILTERS, ids=[str(f) for f in FILTERS])
async def test_db_matches_evaluator(
    client: AsyncClient, seeded: dict[str, dict], raw: dict | None
) -> None:
    r = await client.post("/v1/objects/Row/query", json={"filter": raw})
    assert r.status_code == 200, r.text
    got = {item["id"] for item in r.json()["items"]}
    if raw is None:
        expected = set(seeded)
    else:
        f = parse_filter(raw)
        expected = set()
        for obj_id, row in seeded.items():
            data = {**row, "key": row["trade_id"], "version": 1}
            if matches(f, data):
                expected.add(obj_id)
    assert got == expected
    c = await client.post("/v1/objects/Row/count", json={"filter": raw})
    assert c.json()["count"] == len(expected)


async def test_order_by_offset(client: AsyncClient, seeded: dict[str, dict]) -> None:
    r = await client.post(
        "/v1/objects/Row/query", json={"order_by": [{"field": "key", "desc": True}], "limit": 2}
    )
    assert [i["key"] for i in r.json()["items"]] == ["T5", "T4"]
    r = await client.post(
        "/v1/objects/Row/query", json={"order_by": [{"field": "key"}], "limit": 2, "offset": 3}
    )
    assert [i["key"] for i in r.json()["items"]] == ["T4", "T5"]
    assert r.json()["next_cursor"] is None


async def test_cursor_pagination(client: AsyncClient, seeded: dict[str, dict]) -> None:
    seen: list[str] = []
    cursor = None
    for _ in range(10):
        body = {"limit": 2, "cursor": cursor} if cursor else {"limit": 2}
        r = await client.post("/v1/objects/Row/query", json=body)
        assert r.status_code == 200, r.text
        seen += [i["key"] for i in r.json()["items"]]
        cursor = r.json()["next_cursor"]
        if not cursor:
            break
    assert sorted(seen) == ["T1", "T2", "T3", "T4", "T5"]
    r = await client.post("/v1/objects/Row/query", json={"cursor": "garbage"})
    assert r.status_code == 422


async def test_invalid_filter(client: AsyncClient, seeded: dict[str, dict]) -> None:
    r = await client.post("/v1/objects/Row/query", json={"filter": {"field": "qty", "op": "gt"}})
    assert r.status_code == 422


async def test_order_by_numeric_field_sorts_numerically(
    client: AsyncClient, seeded: dict[str, dict]
) -> None:
    r = await client.post("/v1/objects/Row/query", json={"order_by": [{"field": "qty"}]})
    assert [i["key"] for i in r.json()["items"]] == ["T5", "T4", "T2", "T1", "T3"]
    r = await client.post(
        "/v1/objects/Row/query", json={"order_by": [{"field": "qty", "desc": True}]}
    )
    assert [i["key"] for i in r.json()["items"]] == ["T3", "T1", "T2", "T4", "T5"]
