import pytest

from dbpersist_common.filters import matches, parse_filter

TRADE = {
    "trade_id": "T1",
    "qty": 150,
    "price": 10.5,
    "book": "ALPHA",
    "active": True,
    "note": None,
    "leg": {"qty": 7, "when": "2024-06-01T12:00:00+00:00"},
}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ({"field": "qty", "op": "gt", "value": 100}, True),
        ({"field": "qty", "op": "gt", "value": 150}, False),
        ({"field": "qty", "op": "ge", "value": 150}, True),
        ({"field": "qty", "op": "lt", "value": 100}, False),
        ({"field": "qty", "op": "le", "value": 150}, True),
        ({"field": "qty", "op": "eq", "value": 150}, True),
        ({"field": "qty", "op": "ne", "value": 150}, False),
        ({"field": "price", "op": "gt", "value": 10}, True),
        ({"field": "qty", "op": "eq", "value": "150"}, False),
        ({"field": "book", "op": "like", "value": "AL%"}, True),
        ({"field": "book", "op": "like", "value": "al%"}, False),
        ({"field": "book", "op": "not_like", "value": "A_PHA"}, False),
        ({"field": "book", "op": "like", "value": "%.%"}, False),
        ({"field": "book", "op": "in", "value": ["BETA", "ALPHA"]}, True),
        ({"field": "book", "op": "in", "value": ["BETA"]}, False),
        ({"field": "active", "op": "eq", "value": True}, True),
        ({"field": "active", "op": "eq", "value": 1}, False),
        ({"field": "note", "op": "is_null"}, True),
        ({"field": "missing", "op": "is_null"}, True),
        ({"field": "missing", "op": "is_not_null"}, False),
        ({"field": "missing", "op": "eq", "value": 1}, False),
        ({"field": "leg.qty", "op": "eq", "value": 7}, True),
        ({"field": "leg.nope", "op": "is_null"}, True),
        (
            {
                "field": "leg.when",
                "op": "gt",
                "value": "2024-01-01T00:00:00+00:00",
                "value_type": "datetime",
            },
            True,
        ),
        (
            {
                "field": "leg.when",
                "op": "lt",
                "value": "2024-01-01T00:00:00+00:00",
                "value_type": "datetime",
            },
            False,
        ),
        (
            {
                "and": [
                    {"field": "qty", "op": "gt", "value": 100},
                    {"field": "book", "op": "eq", "value": "ALPHA"},
                ]
            },
            True,
        ),
        (
            {
                "or": [
                    {"field": "qty", "op": "gt", "value": 1000},
                    {"field": "book", "op": "eq", "value": "ALPHA"},
                ]
            },
            True,
        ),
        (
            {
                "and": [
                    {"field": "qty", "op": "gt", "value": 1000},
                    {"field": "book", "op": "eq", "value": "ALPHA"},
                ]
            },
            False,
        ),
    ],
)
def test_matches(raw: dict, expected: bool) -> None:
    assert matches(parse_filter(raw), TRADE) is expected
