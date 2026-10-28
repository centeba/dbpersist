import os
from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from dbpersist_persistence.db import Base
from dbpersist_persistence.main import create_app
from dbpersist_persistence.services import outbox
from dbpersist_persistence.services.events import RecordingPublisher
from dbpersist_persistence.settings import Settings

TEST_DB_URL = os.environ.get(
    "PERSIST_TEST_DATABASE_URL",
    "postgresql+asyncpg://dbpersist:dbpersist@localhost:5442/persist_test",
)
API_KEY = "test-key"
HEADERS = {"X-API-Key": API_KEY}


@pytest.fixture(scope="session")
def settings() -> Settings:
    # Background loops are off: tests drain the inbound queue and the outbox explicitly.
    return Settings(
        persist_database_url=TEST_DB_URL,
        api_key=API_KEY,
        inbound_processor_enabled=False,
        outbox_relay_enabled=False,
    )


@pytest.fixture(scope="session", autouse=True)
async def _schema(settings: Settings) -> AsyncIterator[None]:
    engine = create_async_engine(settings.persist_database_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


@pytest.fixture
async def publisher() -> RecordingPublisher:
    return RecordingPublisher()


async def relay(app: FastAPI) -> int:
    """Hand every unsent outbox row to the app's publisher, as the relay loop would."""
    settings: Settings = app.state.settings
    return await outbox.relay_once(
        app.state.sessionmaker, app.state.publisher, settings.outbox_batch_size
    )


@pytest.fixture
async def app(settings: Settings, publisher: RecordingPublisher) -> AsyncIterator[FastAPI]:
    """A started app on an empty schema. The relay loop is off (see ``settings``)."""
    app = create_app(settings, publisher=publisher)
    async with app.router.lifespan_context(app):
        async with app.state.engine.begin() as conn:
            await conn.execute(
                text(
                    "TRUNCATE api_keys, outbox, inbound_receipts, inbound_messages, "
                    "inbound_sources, objects, object_types CASCADE"
                )
            )
        yield app


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    """API client that relays the outbox after every response, so ``publisher.events`` holds
    exactly the events of the committed changes so far, as it did when routes published directly.
    """

    async def relay_after(_: Response) -> None:
        await relay(app)

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers=HEADERS,
        event_hooks={"response": [relay_after]},
    ) as c:
        yield c


TRADE_SCHEMA = {
    "type": "object",
    "required": ["trade_id", "qty"],
    "properties": {
        "trade_id": {"type": "string"},
        "qty": {"type": "number"},
        "book": {"type": ["string", "null"]},
        "account": {"type": ["string", "null"], "x-ref": "Account"},
    },
}


@pytest.fixture
async def trade_type(client: AsyncClient) -> dict:
    r = await client.post(
        "/v1/types", json={"name": "Trade", "key_fields": ["trade_id"], "json_schema": TRADE_SCHEMA}
    )
    assert r.status_code == 201, r.text
    r = await client.post("/v1/types", json={"name": "Account", "key_fields": ["acct"]})
    assert r.status_code == 201, r.text
    return r.json()
