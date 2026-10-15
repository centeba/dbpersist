import pytest
from pydantic import ValidationError

from dbpersist_common.filters import And, Clause, Or, dump_filter, parse_filter


def test_parse_nested_tree() -> None:
    raw = {
        "and": [
            {"field": "qty", "op": "gt", "value": 100},
            {
                "or": [
                    {"field": "book", "op": "eq", "value": "A"},
                    {"field": "book", "op": "is_null"},
                ]
            },
        ]
    }
    f = parse_filter(raw)
    assert isinstance(f, And)
    assert isinstance(f.and_[0], Clause)
    assert isinstance(f.and_[1], Or)
    assert dump_filter(f) == raw


@pytest.mark.parametrize(
    "raw",
    [
        {"field": "qty", "op": "is_null", "value": 1},
        {"field": "qty", "op": "gt"},
        {"field": "qty", "op": "gt", "value": [1, 2]},
        {"field": "qty", "op": "in", "value": []},
        {"field": "qty", "op": "in", "value": 3},
        {"field": "qty", "op": "like", "value": 3},
        {"field": "bad-name", "op": "eq", "value": 1},
        {"field": "a..b", "op": "eq", "value": 1},
        {"field": "when", "op": "gt", "value": "not a date", "value_type": "datetime"},
        {"and": []},
        {"field": "qty", "op": "eq", "value": 1, "extra": True},
    ],
)
def test_rejects_invalid(raw: dict) -> None:
    with pytest.raises(ValidationError):
        parse_filter(raw)


def test_dotted_path_and_datetime_ok() -> None:
    f = parse_filter(
        {
            "field": "leg.when",
            "op": "ge",
            "value": "2024-01-01T00:00:00+00:00",
            "value_type": "datetime",
        }
    )
    assert isinstance(f, Clause)
    assert f.field == "leg.when"
