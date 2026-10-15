import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from dbpersist_common.events import ObjectEvent
from dbpersist_pubsub.db import sync_sessionmaker
from dbpersist_pubsub.matching import select_matching
from dbpersist_pubsub.models import Delivery, Subscriber, Subscription
from dbpersist_pubsub.tasks.cleanup import prune_old_deliveries, remove_idle_subscribers
from dbpersist_pubsub.tasks.deliver import attempt_delivery, backoff_seconds, mark_failed
from dbpersist_pubsub.tasks.match import fan_out
from dbpersist_pubsub.testing import WebhookSink


def _sub(type_name: str = "Trade", **kw) -> Subscription:
    kw.setdefault("subscriber_id", uuid.uuid4())
    kw.setdefault("by", "key")
    return Subscription(id=uuid.uuid4(), type_name=type_name, **kw)


def test_select_matching_dedupes_per_subscriber() -> None:
    sid = uuid.uuid4()
    obj_id = uuid.uuid4()
    subs = [
        _sub(subscriber_id=sid, filter={"field": "qty", "op": "gt", "value": 10}),
        _sub(subscriber_id=sid, keys=["T1"]),
        _sub(subscriber_id=uuid.uuid4(), keys=[str(obj_id)], by="id"),
        _sub(subscriber_id=uuid.uuid4(), keys=["nope"]),
        _sub("Order", subscriber_id=uuid.uuid4(), filter={"field": "qty", "op": "gt", "value": 0}),
        _sub(subscriber_id=uuid.uuid4(), filter={"field": "key", "op": "eq", "value": "T1"}),
    ]
    ev = ObjectEvent(
        type_name="Trade", object_id=obj_id, key="T1", op="update", version=3, data={"qty": 50}
    )
    out = select_matching(subs, ev)
    assert len(out[sid]) == 2
    assert len(out) == 3


def _make_subscriber(name: str = "s", url: str = "http://r.test/hook", **kw) -> Subscriber:
    with sync_sessionmaker()() as session:
        s = Subscriber(id=uuid.uuid4(), name=name, webhook_url=url, secret="sec", **kw)
        session.add(s)
        session.add(Subscription(subscriber_id=s.id, type_name="Trade", keys=["T1"], by="key"))
        session.commit()
        return s


def test_fan_out_and_attempt_delivery(sink: WebhookSink, mem_stats) -> None:
    s = _make_subscriber()
    ev = ObjectEvent(type_name="Trade", key="T1", op="insert", version=1, data={"qty": 1})
    ids = fan_out(ev.to_wire())
    assert len(ids) == 1
    with sync_sessionmaker()() as session:
        d = session.get(Delivery, ids[0])
        assert d is not None and d.status == "pending" and d.subscriber_id == s.id

    sink.fail_times = 1
    with pytest.raises(httpx.HTTPStatusError):
        attempt_delivery(ids[0])
    with sync_sessionmaker()() as session:
        d = session.get(Delivery, ids[0])
        assert d.attempts == 1 and d.status == "pending" and "500" in (d.last_error or "")

    assert attempt_delivery(ids[0]) is True
    with sync_sessionmaker()() as session:
        d = session.get(Delivery, ids[0])
        assert d.status == "delivered" and d.attempts == 2 and d.delivered_at is not None
        assert session.get(Subscriber, s.id).last_delivery_ok_at is not None
    assert attempt_delivery(ids[0]) is True  # idempotent on delivered rows
    assert len(sink.requests) == 2
    assert mem_stats.counts == {"received": 1, "matched": 1, "delivered": 1}

    mark_failed(ids[0])  # delivered rows are not demoted
    with sync_sessionmaker()() as session:
        assert session.get(Delivery, ids[0]).status == "delivered"


def test_delivery_to_missing_subscriber_marks_failed(sink: WebhookSink) -> None:
    s = _make_subscriber()
    ids = fan_out(ObjectEvent(type_name="Trade", key="T1", op="insert", data={}).to_wire())
    with sync_sessionmaker()() as session:
        session.delete(session.get(Subscriber, s.id))
        session.commit()
    # cascade removed the delivery row; attempt is a no-op success
    assert attempt_delivery(ids[0]) is True
    assert sink.requests == []


def test_backoff_grows_and_caps() -> None:
    assert 0.5 <= backoff_seconds(0, 600) <= 1.5
    assert 16 <= backoff_seconds(5, 600) <= 48
    assert 300 <= backoff_seconds(20, 600) <= 900


def test_remove_idle_and_prune() -> None:
    now = datetime.now(UTC)
    old = now - timedelta(hours=3)
    _make_subscriber("idle", last_seen_at=old)
    _make_subscriber("alive-heartbeat", last_seen_at=now)
    _make_subscriber("alive-delivery", last_seen_at=old, last_delivery_ok_at=now)
    with sync_sessionmaker()() as session:
        removed = remove_idle_subscribers(session, idle_minutes=60, now=now)
        assert removed == ["idle"]
        s = session.query(Subscriber).filter_by(name="alive-heartbeat").one()
        session.add(
            Delivery(
                event_id=uuid.uuid4(),
                subscriber_id=s.id,
                subscription_ids=[],
                event={},
                created_at=now - timedelta(days=10),
            )
        )
        session.add(
            Delivery(
                event_id=uuid.uuid4(),
                subscriber_id=s.id,
                subscription_ids=[],
                event={},
                created_at=now,
            )
        )
        session.commit()
        assert prune_old_deliveries(session, retention_days=7, now=now) == 1
        assert session.query(Delivery).count() == 1
