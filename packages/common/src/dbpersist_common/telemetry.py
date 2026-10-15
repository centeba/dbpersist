"""Logging and OpenTelemetry setup shared by every dbpersist process.

What you get after ``init_telemetry(service_name, settings)``:

- structured JSON logs on stdout (or plain text with ``LOG_FORMAT=text``), every record carrying
  ``trace_id`` / ``span_id`` when written inside a span, plus any ``extra={...}`` fields;
- when ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set: traces, metrics and logs exported over OTLP/HTTP
  to that collector (the observability stack's ``otel-collector:4318``);
- httpx clients instrumented globally, so outgoing calls propagate ``traceparent``.

FastAPI apps, SQLAlchemy engines and Celery are instrumented with the ``instrument_*`` helpers.
"""

import json
import logging
import os
import sys
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Request, Response
from opentelemetry import metrics, trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import Resource
from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.middleware.base import BaseHTTPMiddleware

from dbpersist_common.settings import CommonSettings

_STANDARD_ATTRS = frozenset(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys()
    | {"message", "asctime", "taskName"}
)
_QUIET_PATHS = frozenset({"/health", "/healthz"})
_initialised: set[str] = set()


def _current_ids() -> tuple[str | None, str | None]:
    ctx = trace.get_current_span().get_span_context()
    if not ctx.is_valid:
        return None, None
    return format(ctx.trace_id, "032x"), format(ctx.span_id, "016x")


class JsonFormatter(logging.Formatter):
    """One JSON object per line; extra fields and trace ids included."""

    def __init__(self, service_name: str) -> None:
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        trace_id, span_id = _current_ids()
        out: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "service": self.service_name,
            "message": record.getMessage(),
        }
        if trace_id:
            out["trace_id"] = trace_id
            out["span_id"] = span_id
        for k, v in record.__dict__.items():
            if k not in _STANDARD_ATTRS and not k.startswith("_"):
                out[k] = v if isinstance(v, str | int | float | bool | type(None)) else repr(v)
        if record.exc_info:
            out["exception"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


class TextFormatter(logging.Formatter):
    def __init__(self, service_name: str) -> None:
        super().__init__(f"%(asctime)s %(levelname)-5s {service_name} %(name)s: %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        trace_id, _ = _current_ids()
        return f"{base} trace_id={trace_id}" if trace_id else base


def configure_logging(service_name: str, settings: CommonSettings) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    fmt: logging.Formatter = (
        JsonFormatter(service_name)
        if settings.log_format == "json"
        else TextFormatter(service_name)
    )
    handler.setFormatter(fmt)
    root.addHandler(handler)
    root.setLevel(settings.log_level.upper())
    # the request-log middleware below replaces uvicorn's access line
    logging.getLogger("uvicorn.access").disabled = True
    for noisy in ("uvicorn", "uvicorn.error", "celery", "kombu", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(max(logging.WARNING, root.level))


def _resource(service_name: str, settings: CommonSettings) -> Resource:
    return Resource.create(
        {
            "service.name": service_name,
            "service.version": settings.otel_service_version,
            "deployment.environment": settings.deployment_environment,
        }
    )


def init_telemetry(service_name: str, settings: CommonSettings) -> bool:
    """Configure logging and, when an OTLP endpoint is set, export traces/metrics/logs.

    Idempotent per service name. Returns True when OTLP export is active.
    """
    service_name = settings.otel_service_name or service_name
    # keyed by pid: a prefork Celery child inherits the parent's module state but needs its own
    # exporter threads, so it must run the setup again after the fork
    marker = f"{os.getpid()}:{service_name}"
    if marker in _initialised:
        return bool(settings.otel_exporter_otlp_endpoint)
    _initialised.add(marker)

    configure_logging(service_name, settings)
    HTTPXClientInstrumentor().instrument()

    endpoint = settings.otel_exporter_otlp_endpoint.rstrip("/")
    if not endpoint:
        logging.getLogger(__name__).info("telemetry: OTLP export disabled (no endpoint)")
        return False

    from opentelemetry._logs import set_logger_provider
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = _resource(service_name, settings)

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces"))
    )
    trace.set_tracer_provider(tracer_provider)

    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=f"{endpoint}/v1/metrics"), export_interval_millis=15_000
    )
    metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[reader]))

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(OTLPLogExporter(endpoint=f"{endpoint}/v1/logs"))
    )
    set_logger_provider(logger_provider)
    logging.getLogger().addHandler(
        LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
    )

    logging.getLogger(__name__).info(
        "telemetry: exporting to %s", endpoint, extra={"otlp_endpoint": endpoint}
    )
    return True


def instrument_fastapi(app: FastAPI) -> None:
    FastAPIInstrumentor.instrument_app(app, excluded_urls="health,healthz")
    app.add_middleware(RequestLogMiddleware)


def instrument_sqlalchemy(engine: Engine | AsyncEngine) -> None:
    sync_engine = engine.sync_engine if isinstance(engine, AsyncEngine) else engine
    SQLAlchemyInstrumentor().instrument(engine=sync_engine, enable_commenter=False)


def instrument_celery() -> None:
    from opentelemetry.instrumentation.celery import CeleryInstrumentor

    CeleryInstrumentor().instrument()


def get_meter(name: str) -> metrics.Meter:
    return metrics.get_meter(name)


def get_tracer(name: str) -> trace.Tracer:
    return trace.get_tracer(name)


class RequestLogMiddleware(BaseHTTPMiddleware):
    """One structured log line per request with method, path, status and latency."""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if request.url.path in _QUIET_PATHS:
            return await call_next(request)
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            logging.getLogger("dbpersist.http").log(
                logging.WARNING if status >= 500 else logging.INFO,
                "%s %s -> %s (%.1f ms)",
                request.method,
                request.url.path,
                status,
                duration_ms,
                extra={
                    "http_method": request.method,
                    "http_path": request.url.path,
                    "http_status": status,
                    "duration_ms": duration_ms,
                    "client_ip": request.client.host if request.client else None,
                },
            )
