"""PUBSUB_ENABLED=false: persistence runs alone, writes succeed, nothing is sent anywhere."""

from httpx import ASGITransport, AsyncClient

from dbpersist_persistence.main import create_app
from dbpersist_persistence.services.events import NullPublisher
from dbpersist_persistence.settings import Settings

from .conftest import API_KEY, HEADERS, TEST_DB_URL


async def test_persistence_without_pubsub() -> None:
    settings = Settings(
        persist_database_url=TEST_DB_URL,
        api_key=API_KEY,
        inbound_processor_enabled=False,
        pubsub_enabled=False,
        redis_url="redis://127.0.0.1:1/0",  # unreachable on purpose: it must never be used
    )
    app = create_app(settings)
    assert isinstance(app.state.publisher, NullPublisher)
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test", headers=HEADERS
        ) as c:
            await c.post("/v1/types", json={"name": "Solo", "key_fields": ["k"]})
            r = await c.post("/v1/objects/Solo", json={"k": "a", "v": 1})
            assert r.status_code == 201, r.text
            oid = r.json()["id"]
            r = await c.patch(f"/v1/objects/Solo/{oid}", json={"version": 1, "data": {"v": 2}})
            assert r.status_code == 200
            r = await c.post(
                "/v1/transactions",
                json={"ops": [{"op": "insert", "type": "Solo", "data": {"k": "b"}}]},
            )
            assert r.status_code == 200
            r = await c.post(
                "/v1/publish", json={"type": "Solo", "op": "update", "data": {"k": "a"}}
            )
            assert r.status_code == 202
            assert (await c.get("/healthz")).status_code == 200
    assert app.state.publisher.dropped == 4  # insert, update, transaction insert, publish-only


def test_default_is_enabled() -> None:
    assert Settings(persist_database_url=TEST_DB_URL).pubsub_enabled is True
