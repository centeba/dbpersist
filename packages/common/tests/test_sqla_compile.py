from sqlalchemy import Column, Integer, MetaData, String, Table, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB

from dbpersist_common.filters import parse_filter, to_sqla

md = MetaData()
t = Table(
    "objects",
    md,
    Column("id", String),
    Column("key", String),
    Column("version", Integer),
    Column("data", JSONB),
)
COLS = {"id": t.c.id, "key": t.c.key, "version": t.c.version}


def compile_(raw: dict) -> str:
    expr = to_sqla(parse_filter(raw), t.c.data, COLS)
    stmt = select(t.c.id).where(expr)
    return str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


def test_numeric_cast_and_containment() -> None:
    sql = compile_({"field": "qty", "op": "gt", "value": 100})
    assert "CAST((objects.data #>> '{qty}') AS NUMERIC) > 100" in sql


def test_eq_uses_containment() -> None:
    sql = compile_({"field": "leg.qty", "op": "eq", "value": 7})
    assert "@>" in sql
    assert """@> CAST('{"leg": {"qty": 7}}' AS JSONB)""" in sql


def test_string_like_and_null() -> None:
    sql = compile_(
        {
            "and": [
                {"field": "book", "op": "like", "value": "AL%"},
                {"field": "note", "op": "is_null"},
            ]
        }
    )
    assert "LIKE 'AL%%'" in sql  # literal_binds doubles % for the DBAPI paramstyle
    assert "IS NULL" in sql
    assert "CAST" not in sql


def test_column_field() -> None:
    sql = compile_({"field": "version", "op": "ge", "value": 3})
    assert "objects.version >= 3" in sql
    assert "#>>" not in sql


def test_or_and_in() -> None:
    sql = compile_(
        {
            "or": [
                {"field": "book", "op": "in", "value": ["A", "B"]},
                {"field": "active", "op": "eq", "value": True},
            ]
        }
    )
    assert " IN ('A', 'B')" in sql
    assert " OR " in sql
    assert "AS BOOLEAN) = true" in sql
