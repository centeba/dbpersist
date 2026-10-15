import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import redis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from dbpersist_common.telemetry import (
    init_telemetry,
    instrument_celery,
    instrument_fastapi,
    instrument_sqlalchemy,
)
from dbpersist_pubsub.db import make_async_engine, make_async_sessionmaker
from dbpersist_pubsub.routers import admin, publish, subscribers, subscriptions
from dbpersist_pubsub.settings import Settings

log = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    init_telemetry("dbpersist-pubsub", settings)
    instrument_celery()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = make_async_engine(settings.pubsub_database_url)
        instrument_sqlalchemy(engine)
        app.state.engine = engine
        app.state.sessionmaker = make_async_sessionmaker(engine)
        yield
        await engine.dispose()

    app = FastAPI(title="dbpersist pubsub", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    for r in (subscribers.router, subscriptions.router, publish.router, admin.router):
        app.include_router(r, prefix="/v1")
    instrument_fastapi(app)

    @app.get("/health")
    async def health() -> dict[str, str]:
        """Liveness: the process is up."""
        return {"status": "ok"}

    @app.get("/healthz")
    async def healthz(request: Request) -> JSONResponse:
        """Readiness: database and broker answer. Probed by the observability gateway."""
        checks: dict[str, str] = {}
        try:
            async with request.app.state.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            checks["database"] = "up"
        except Exception as exc:  # noqa: BLE001
            log.warning("healthz: database check failed", extra={"error": repr(exc)})
            checks["database"] = "down"
        try:
            await asyncio.to_thread(redis.Redis.from_url(settings.redis_url, socket_timeout=2).ping)
            checks["broker"] = "up"
        except Exception as exc:  # noqa: BLE001
            log.warning("healthz: broker check failed", extra={"error": repr(exc)})
            checks["broker"] = "down"
        ok = all(v == "up" for v in checks.values())
        return JSONResponse(
            {"status": "ok" if ok else "unhealthy", **checks}, status_code=200 if ok else 503
        )

    return app


app = create_app()
