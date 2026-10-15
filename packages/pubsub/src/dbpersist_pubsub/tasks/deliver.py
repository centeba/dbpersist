"""pubsub.deliver_webhook: signed POST to the subscriber with exponential-backoff retries."""

import json
import logging
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import httpx
from celery import Task
from sqlalchemy import bindparam, select, text

from dbpersist_common.signing import EVENT_ID_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER, sign
from dbpersist_common.telemetry import get_meter
from dbpersist_pubsub.celery_app import celery_app, settings
from dbpersist_pubsub.db import autocommit_sessionmaker, sync_sessionmaker
from dbpersist_pubsub.models import Delivery, Subscriber
from dbpersist_pubsub.stats import get_stats

log = logging.getLogger(__name__)

_meter = get_meter("dbpersist.pubsub")
_attempts = _meter.create_counter(
    "dbpersist.pubsub.delivery.attempts", unit="1", description="Webhook POST attempts by outcome"
)
_duration = _meter.create_histogram(
    "dbpersist.pubsub.delivery.duration", unit="ms", description="Webhook POST round-trip time"
)


def make_client() -> httpx.Client:
    """Factory for the HTTP client; tests replace it with a MockTransport-backed client."""
    return httpx.Client(
        timeout=settings.webhook_timeout_seconds,
        limits=httpx.Limits(max_connections=64, max_keepalive_connections=32),
    )


_shared_client: httpx.Client | None = None
_shared_factory: Callable[[], httpx.Client] | None = None


def shared_client() -> httpx.Client:
    """One keep-alive client per worker process, shared by all threads.

    Rebuilt whenever `make_client` is replaced (tests swap in a MockTransport) and the
    cached client was built by a different factory.
    """
    global _shared_client, _shared_factory
    if _shared_client is None or _shared_factory is not make_client:
        if _shared_client is not None:
            _shared_client.close()
        _shared_client = make_client()
        _shared_factory = make_client
    return _shared_client


def build_request(delivery: Delivery, subscriber: Subscriber) -> tuple[bytes, dict[str, str]]:
    payload = {
        "event": delivery.event,
        "subscription_ids": [str(s) for s in delivery.subscription_ids],
        "delivery_id": delivery.id,
    }
    body = json.dumps(payload, separators=(",", ":")).encode()
    ts = str(int(time.time()))
    headers = {
        "Content-Type": "application/json",
        TIMESTAMP_HEADER: ts,
        SIGNATURE_HEADER: sign(subscriber.secret, ts, body),
        EVENT_ID_HEADER: str(delivery.event_id),
    }
    return body, headers


_LOAD = (
    select(Delivery, Subscriber)
    .join(Subscriber, Subscriber.id == Delivery.subscriber_id, isouter=True)
    .where(Delivery.id == bindparam("id"))
)
# The subscriber's last_delivery_ok_at is refreshed at most once per second: every delivery
# to a subscriber touches the same row, and with concurrent workers that single-row lock was
# the throughput ceiling (see docs/DEVELOPER_GUIDE.md "Performance"). One-second resolution
# is plenty for idle detection (minutes) and the admin view.
_RECORD_SUCCESS = text(
    """
    WITH d AS (
        UPDATE deliveries
           SET status = 'delivered', delivered_at = :now, last_error = NULL,
               attempts = attempts + 1
         WHERE id = :id
     RETURNING subscriber_id
    )
    UPDATE subscribers SET last_delivery_ok_at = :now
     WHERE id IN (SELECT subscriber_id FROM d)
       AND (last_delivery_ok_at IS NULL OR last_delivery_ok_at < :stale)
    """
)
_RECORD_ERROR = text(
    "UPDATE deliveries SET attempts = attempts + 1, last_error = :err WHERE id = :id"
)
_RECORD_ORPHAN = text(
    "UPDATE deliveries SET status = 'failed', last_error = 'subscriber removed' WHERE id = :id"
)


def attempt_delivery(delivery_id: int, client: httpx.Client | None = None) -> bool:
    """One delivery attempt. Returns True on success; raises httpx.HTTPError on failure.

    Updates attempts/last_error on the Delivery row either way. Runs on the autocommit engine:
    one SELECT (delivery joined to its subscriber) and one UPDATE, so two round trips per attempt.
    """
    with autocommit_sessionmaker()() as session:
        row = session.execute(_LOAD, {"id": delivery_id}).first()
        if row is None:
            return True
        delivery, subscriber = row.tuple()
        if delivery.status == "delivered":
            return True
        if subscriber is None:
            session.execute(_RECORD_ORPHAN, {"id": delivery_id})
            return True
        body, headers = build_request(delivery, subscriber)
        attempt = delivery.attempts + 1
        started = time.perf_counter()
        log_fields = {
            "delivery_id": delivery.id,
            "event_id": str(delivery.event_id),
            "subscriber": subscriber.name,
            "attempt": attempt,
        }
        try:
            http = client or shared_client()
            resp = http.post(subscriber.webhook_url, content=body, headers=headers)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            elapsed = (time.perf_counter() - started) * 1000
            _attempts.add(1, {"outcome": "error"})
            _duration.record(elapsed, {"outcome": "error"})
            err = f"{type(exc).__name__}: {exc}"[:2000]
            session.execute(_RECORD_ERROR, {"id": delivery_id, "err": err})
            log.warning(
                "delivery attempt failed: %s",
                exc,
                extra={**log_fields, "duration_ms": round(elapsed, 2), "error": type(exc).__name__},
            )
            raise
        elapsed = (time.perf_counter() - started) * 1000
        _attempts.add(1, {"outcome": "ok"})
        _duration.record(elapsed, {"outcome": "ok"})
        log.info(
            "delivered",
            extra={**log_fields, "duration_ms": round(elapsed, 2), "http_status": resp.status_code},
        )
        now = datetime.now(UTC)
        session.execute(
            _RECORD_SUCCESS, {"id": delivery_id, "now": now, "stale": now - timedelta(seconds=1)}
        )
    get_stats().incr("delivered")
    return True


def mark_failed(delivery_id: int) -> None:
    with sync_sessionmaker()() as session:
        delivery = session.get(Delivery, delivery_id)
        if delivery is not None and delivery.status != "delivered":
            delivery.status = "failed"
            session.commit()
    get_stats().incr("failed")


def backoff_seconds(retries: int, cap: int) -> float:
    base = min(cap, 2**retries)
    return base * random.uniform(0.5, 1.5)  # jitter


@celery_app.task(name="pubsub.deliver_webhook", bind=True, max_retries=settings.webhook_max_retries)
def deliver_webhook(self: Task, delivery_id: int) -> bool:
    try:
        return attempt_delivery(delivery_id)
    except httpx.HTTPError as exc:
        if self.request.retries >= self.max_retries:
            log.error(
                "delivery failed permanently: %s",
                exc,
                extra={"delivery_id": delivery_id, "retries": self.request.retries},
            )
            mark_failed(delivery_id)
            return False
        countdown = backoff_seconds(self.request.retries, settings.webhook_backoff_max_seconds)
        raise self.retry(exc=exc, countdown=countdown) from exc
