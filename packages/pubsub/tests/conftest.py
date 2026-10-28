import os
from collections.abc import AsyncIterator, Iterator

# Point every engine (async API + sync Celery tasks) at the test database before any import.
os.environ.setdefault(
    "PUBSUB_DATABASE_URL", "postgresql+asyncpg://dbpersist:dbpersist@localhost:5442/pubsub_test"
)
os.environ.setdefault(
    "PUBSUB_SYNC_DATABASE_URL",
    "postgresql+psycopg://dbpersist:dbpersist@localhost:5442/pubsub_test",
)

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from dbpersist_pubsub import stats as stats_mod
from dbpersist_pubsub.celery_app import celery_app
from dbpersist_pubsub.db import Base, sync_engine
from dbpersist_pubsub.main import create_app
from dbpersist_pubsub.settings import Settings
from dbpersist_pubsub.tasks import deliver
from dbpersist_pubsub.testing import WebhookSink

HEADERS = {"X-API-Key": "test-key"}


@pytest.fixture(scope="session", autouse=True)
def _celery_eager() -> None:
    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = False  # let apply() re-run Retry instead of raising it


@pytest.fixture(scope="session", autouse=True)
def _schema() -> Iterator[None]:
    Base.metadata.drop_all(sync_engine())
    Base.metadata.create_all(sync_engine())
    yield


@pytest.fixture(autouse=True)
def _clean_tables() -> None:
    with sync_engine().begin() as conn:
        conn.execute(text("TRUNCATE api_keys, deliveries, subscriptions, subscribers CASCADE"))


@pytest.fixture(autouse=True)
def mem_stats() -> stats_mod.MemoryStats:
    s = stats_mod.MemoryStats()
    stats_mod.set_stats(s)
    return s


@pytest.fixture
def sink(monkeypatch: pytest.MonkeyPatch) -> WebhookSink:
    s = WebhookSink()
    monkeypatch.setattr(
        deliver, "make_client", lambda: httpx.Client(transport=httpx.MockTransport(s.handler))
    )
    return s


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    app = create_app(Settings(api_key="test-key"))
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test", headers=HEADERS
        ) as c:
            yield c


@pytest.fixture
async def subscriber(client: AsyncClient) -> dict:
    r = await client.post(
        "/v1/subscribers", json={"name": "app1", "webhook_url": "http://receiver.test/webhook"}
    )
    assert r.status_code == 201, r.text
    return r.json()
