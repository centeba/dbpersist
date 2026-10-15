import json
import logging

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from dbpersist_common.settings import CommonSettings
from dbpersist_common.telemetry import JsonFormatter, RequestLogMiddleware, configure_logging


def _format(record: logging.LogRecord) -> dict:
    return json.loads(JsonFormatter("svc").format(record))


def test_json_formatter_includes_extras_and_exception() -> None:
    logger = logging.getLogger("t.fmt")
    rec = logger.makeRecord(
        "t.fmt",
        logging.INFO,
        __file__,
        1,
        "hello %s",
        ("world",),
        None,
        extra={"event_id": "e1", "n": 3},
    )
    out = _format(rec)
    assert out["message"] == "hello world" and out["level"] == "INFO" and out["service"] == "svc"
    assert out["event_id"] == "e1" and out["n"] == 3 and "trace_id" not in out
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        rec = logger.makeRecord("t.fmt", logging.ERROR, __file__, 1, "bad", (), sys.exc_info())
    assert "ValueError: boom" in _format(rec)["exception"]


def test_json_formatter_carries_trace_ids_inside_span() -> None:
    provider = TracerProvider()
    tracer = provider.get_tracer("t")
    with tracer.start_as_current_span("op") as span:
        rec = logging.getLogger("t").makeRecord("t", logging.INFO, __file__, 1, "in span", (), None)
        out = _format(rec)
        ctx = span.get_span_context()
    assert out["trace_id"] == format(ctx.trace_id, "032x") and out["span_id"] == format(
        ctx.span_id, "016x"
    )
    assert trace.get_current_span().get_span_context().is_valid is False


async def test_request_log_middleware_logs_once_per_request(caplog) -> None:
    app = FastAPI()
    app.add_middleware(RequestLogMiddleware)

    @app.get("/x")
    async def x() -> dict[str, int]:
        return {"ok": 1}

    @app.get("/health")
    async def health() -> dict[str, int]:
        return {"ok": 1}

    with caplog.at_level(logging.INFO, logger="dbpersist.http"):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            assert (await c.get("/x")).status_code == 200
            assert (await c.get("/health")).status_code == 200
    http_logs = [r for r in caplog.records if r.name == "dbpersist.http"]
    assert len(http_logs) == 1
    assert http_logs[0].http_path == "/x" and http_logs[0].http_status == 200  # type: ignore[attr-defined]


def test_configure_logging_text_and_json(capsys) -> None:
    configure_logging("svc", CommonSettings(log_format="json", log_level="DEBUG"))
    logging.getLogger("t.cfg").info("json line", extra={"k": "v"})
    out = capsys.readouterr().out.strip().splitlines()[-1]
    assert json.loads(out)["k"] == "v"
    configure_logging("svc", CommonSettings(log_format="text"))
    logging.getLogger("t.cfg").warning("text line")
    assert "text line" in capsys.readouterr().out
    logging.getLogger().handlers.clear()
