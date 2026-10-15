"""pubsub.deliver_webhook: signed POST to the subscriber with exponential-backoff retries."""

import json
import logging
import random
import time
from datetime import UTC, datetime

import httpx
from celery import Task

from dbpersist_common.signing import EVENT_ID_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER, sign
from dbpersist_common.telemetry import get_meter
from dbpersist_pubsub.celery_app import celery_app, settings
from dbpersist_pubsub.db import sync_sessionmaker
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
    return httpx.Client(timeout=settings.webhook_timeout_seconds)


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


def attempt_delivery(delivery_id: int, client: httpx.Client | None = None) -> bool:
    """One delivery attempt. Returns True on success; raises httpx.HTTPError on failure.

    Updates attempts/last_error on the Delivery row either way.
    """
    with sync_sessionmaker()() as session:
        delivery = session.get(Delivery, delivery_id)
        if delivery is None or delivery.status == "delivered":
            return True
        subscriber = session.get(Subscriber, delivery.subscriber_id)
        if subscriber is None:
            delivery.status = "failed"
            delivery.last_error = "subscriber removed"
            session.commit()
            return True
        body, headers = build_request(delivery, subscriber)
        delivery.attempts += 1
        started = time.perf_counter()
        log_fields = {
            "delivery_id": delivery.id,
            "event_id": str(delivery.event_id),
            "subscriber": subscriber.name,
            "attempt": delivery.attempts,
        }
        try:
            http = client or make_client()
            try:
                resp = http.post(subscriber.webhook_url, content=body, headers=headers)
                resp.raise_for_status()
            finally:
                if client is None:
                    http.close()
        except httpx.HTTPError as exc:
            elapsed = (time.perf_counter() - started) * 1000
            _attempts.add(1, {"outcome": "error"})
            _duration.record(elapsed, {"outcome": "error"})
            delivery.last_error = f"{type(exc).__name__}: {exc}"[:2000]
            session.commit()
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
        delivery.status = "delivered"
        delivery.delivered_at = now
        delivery.last_error = None
        subscriber.last_delivery_ok_at = now
        session.commit()
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
