"""FastAPI router that receives signed webhooks and dispatches to the registered callbacks.

Mount it in your own app::

    app.include_router(client.receiver(), prefix="/dbpersist")

and register the subscriber with ``webhook_url="http://host:port/dbpersist/webhook"``.
"""

import logging

from fastapi import APIRouter, HTTPException, Request

from dbpersist_common.signing import SIGNATURE_HEADER, TIMESTAMP_HEADER, verify
from dbpersist_sdk.client import DBPersistClient
from dbpersist_sdk.exceptions import DeliveryError
from dbpersist_sdk.models import Callbacks, ObjectRecord, WebhookPayload

log = logging.getLogger(__name__)


async def dispatch(client: DBPersistClient, payload: WebhookPayload) -> int:
    """Invoke each distinct Callbacks object once per event. Returns the number invoked."""
    ev = payload.event
    record = ObjectRecord.from_event(ev)
    if client.cache is not None:
        if ev.op == "delete":
            client.cache.pop((ev.type_name, ev.key), None)
        else:
            client.cache[(ev.type_name, ev.key)] = record

    seen: list[Callbacks] = []
    for sid in payload.subscription_ids:
        cb = client.callbacks.get(sid)
        if cb is None or any(cb is s for s in seen):
            continue
        seen.append(cb)
    for cb in seen:
        try:
            if ev.op == "insert":
                await cb.on_insert(record)
            elif ev.op == "update":
                await cb.on_update(record, list(ev.changed_fields))
            else:
                await cb.on_delete(record)
        except Exception as exc:  # a buggy callback must not make pubsub retry forever
            log.exception("callback failed for event %s", ev.event_id)
            try:
                await cb.on_error(DeliveryError(f"callback raised: {exc!r}"))
            except Exception:
                log.exception("on_error failed for event %s", ev.event_id)
    return len(seen)


def make_receiver(client: DBPersistClient) -> APIRouter:
    router = APIRouter(tags=["dbpersist-webhook"])

    @router.post("/webhook")
    async def webhook(request: Request) -> dict[str, int]:
        if client.secret is None:
            raise HTTPException(503, "subscriber not registered")
        body = await request.body()
        ts = request.headers.get(TIMESTAMP_HEADER, "")
        sig = request.headers.get(SIGNATURE_HEADER, "")
        if not verify(client.secret, ts, body, sig):
            raise HTTPException(401, "bad signature")
        payload = WebhookPayload.model_validate_json(body)
        n = await dispatch(client, payload)
        return {"dispatched": n}

    return router


def _receiver(self: DBPersistClient) -> APIRouter:
    return make_receiver(self)


DBPersistClient.receiver = _receiver  # type: ignore[attr-defined]
