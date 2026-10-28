import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from dbpersist_common import apikeys
from dbpersist_common.telemetry import (
    init_telemetry,
    instrument_celery,
    instrument_fastapi,
    instrument_sqlalchemy,
)
from dbpersist_persistence.celery_app import make_producer
from dbpersist_persistence.db import get_session, make_engine, make_sessionmaker
from dbpersist_persistence.models import ApiKey
from dbpersist_persistence.routers import inbound, objects, publish, transactions, types
from dbpersist_persistence.services.errors import (
    DuplicateKey,
    InvalidData,
    NotFound,
    VersionConflict,
)
from dbpersist_persistence.services.events import (
    CeleryEventPublisher,
    EventPublisher,
    NullPublisher,
)
from dbpersist_persistence.services.inbound_queue import run_processor
from dbpersist_persistence.services.outbox import run_relay
from dbpersist_persistence.settings import Settings

log = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None, publisher: EventPublisher | None = None
) -> FastAPI:
    settings = settings or Settings()
    init_telemetry("dbpersist-persistence", settings)
    instrument_celery()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = make_engine(
            settings.persist_database_url, settings.db_pool_size, settings.db_max_overflow
        )
        instrument_sqlalchemy(engine)
        app.state.engine = engine
        app.state.sessionmaker = make_sessionmaker(engine)
        apikeys.install_key_store(
            app.state, app.state.sessionmaker, ApiKey, settings.api_key_cache_seconds
        )
        log.info(
            "persistence started", extra={"database": settings.persist_database_url.split("@")[-1]}
        )
        tasks: list[asyncio.Task[None]] = []
        if settings.inbound_processor_enabled:
            tasks.append(asyncio.create_task(run_processor(app.state.sessionmaker, settings)))
        if settings.pubsub_enabled and settings.outbox_relay_enabled:
            tasks.append(
                asyncio.create_task(
                    run_relay(app.state.sessionmaker, settings, app.state.publisher)
                )
            )
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            await engine.dispose()

    app = FastAPI(title="dbpersist persistence", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    if publisher is not None:
        app.state.publisher = publisher
    elif settings.pubsub_enabled:
        app.state.publisher = CeleryEventPublisher(make_producer(settings.redis_url))
    else:
        log.warning("pub/sub disabled (PUBSUB_ENABLED=false): no change events are staged")
        app.state.publisher = NullPublisher()

    for r in (
        types.router,
        objects.router,
        transactions.router,
        publish.router,
        inbound.ingress,
        inbound.manage,
        apikeys.make_router(ApiKey, get_session),
    ):
        app.include_router(r, prefix="/v1")
    instrument_fastapi(app)

    @app.exception_handler(NotFound)
    async def _nf(_: Request, exc: NotFound) -> JSONResponse:
        return JSONResponse({"code": "not_found", "detail": str(exc)}, status_code=404)

    @app.exception_handler(DuplicateKey)
    async def _dk(_: Request, exc: DuplicateKey) -> JSONResponse:
        return JSONResponse(
            {"code": "duplicate_key", "detail": str(exc), "type": exc.type_name, "key": exc.key},
            status_code=409,
        )

    @app.exception_handler(VersionConflict)
    async def _vc(_: Request, exc: VersionConflict) -> JSONResponse:
        return JSONResponse(
            {"code": "version_conflict", "detail": str(exc), "current_version": exc.current},
            status_code=409,
        )

    @app.exception_handler(InvalidData)
    async def _inv(_: Request, exc: InvalidData) -> JSONResponse:
        return JSONResponse({"code": "invalid_data", "detail": exc.message}, status_code=422)

    @app.get("/health")
    async def health() -> dict[str, str]:
        """Liveness: the process is up."""
        return {"status": "ok"}

    @app.get("/healthz")
    async def healthz(request: Request) -> JSONResponse:
        """Readiness: the database answers. Probed by the observability gateway fleet status."""
        try:
            async with request.app.state.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        except Exception as exc:  # noqa: BLE001
            log.warning("healthz failed", extra={"error": repr(exc)})
            return JSONResponse({"status": "unhealthy", "database": "down"}, status_code=503)
        return JSONResponse({"status": "ok", "database": "up"})

    return app


app = create_app()
