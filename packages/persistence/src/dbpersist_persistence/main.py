import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from dbpersist_common.telemetry import (
    init_telemetry,
    instrument_celery,
    instrument_fastapi,
    instrument_sqlalchemy,
)
from dbpersist_persistence.celery_app import make_producer
from dbpersist_persistence.db import make_engine, make_sessionmaker
from dbpersist_persistence.routers import objects, publish, transactions, types
from dbpersist_persistence.services.errors import (
    DuplicateKey,
    InvalidData,
    NotFound,
    VersionConflict,
)
from dbpersist_persistence.services.events import CeleryEventPublisher, EventPublisher
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
        log.info(
            "persistence started", extra={"database": settings.persist_database_url.split("@")[-1]}
        )
        yield
        await engine.dispose()

    app = FastAPI(title="dbpersist persistence", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    app.state.publisher = publisher or CeleryEventPublisher(make_producer(settings.redis_url))

    for r in (types.router, objects.router, transactions.router, publish.router):
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
