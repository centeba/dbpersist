"""Decide which subscribers an event goes to.

Subscriptions are loaded by type and evaluated in memory with the shared filter
evaluator (filter subscriptions) or by key/id membership, then deduplicated per subscriber.
"""

import uuid
from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.orm import Session

from dbpersist_common.events import ObjectEvent
from dbpersist_common.filters import matches, parse_filter
from dbpersist_pubsub.models import Subscription


def subscription_matches(sub: Subscription, event: ObjectEvent) -> bool:
    if sub.type_name != event.type_name:
        return False
    if sub.keys is not None:
        if sub.by == "id":
            return event.object_id is not None and str(event.object_id) in set(sub.keys)
        return event.key in set(sub.keys)
    if sub.filter is None:
        return False
    data = {**event.data, "key": event.key, "version": event.version}
    if event.object_id is not None:
        data["id"] = str(event.object_id)
    try:
        return matches(parse_filter(sub.filter), data)
    except ValueError:
        return False


def select_matching(
    subs: list[Subscription], event: ObjectEvent
) -> dict[uuid.UUID, list[uuid.UUID]]:
    """Map subscriber_id -> matching subscription ids (one delivery per subscriber)."""
    out: dict[uuid.UUID, list[uuid.UUID]] = defaultdict(list)
    for sub in subs:
        if subscription_matches(sub, event):
            out[sub.subscriber_id].append(sub.id)
    return dict(out)


def load_subscriptions(session: Session, type_name: str) -> list[Subscription]:
    stmt = select(Subscription).where(Subscription.type_name == type_name)
    return list(session.execute(stmt).scalars())


def match_event(session: Session, event: ObjectEvent) -> dict[uuid.UUID, list[uuid.UUID]]:
    return select_matching(load_subscriptions(session, event.type_name), event)
