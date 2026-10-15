"""The subscriber side: a FastAPI service that keeps a live view of open orders.

What it shows:

- ``active_select`` to load the current open orders *and* subscribe to changes that match the
  same filter, in one call;
- a key subscription for the orders of VIP customers, so the desk sees every change to those
  regardless of status;
- the SDK webhook receiver mounted into the app, verifying HMAC signatures for us;
- callbacks that reconcile by ``version`` so late or duplicate deliveries never regress state;
- a heartbeat task so the pub/sub service knows this subscriber is alive.

Run with ``uv run uvicorn orderdesk.dashboard:app --host 0.0.0.0 --port 9200`` and open
http://localhost:9200/ for a JSON view.
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import FastAPI

from dbpersist_sdk import BaseCallbacks, DBPersistClient, ObjectRecord
from orderdesk import config
from orderdesk.domain import CUSTOMER, ORDER, ensure_types

log = logging.getLogger("orderdesk.dashboard")


class Desk(BaseCallbacks):
    """In-memory state fed by webhooks. One instance serves every subscription."""

    def __init__(self) -> None:
        self.open_orders: dict[str, ObjectRecord] = {}
        self.vip_activity: list[dict] = []
        self.log: list[dict] = []
        self.errors: list[str] = []

    def _note(self, kind: str, record: ObjectRecord, extra: dict | None = None) -> None:
        entry = {
            "at": datetime.now(UTC).isoformat(timespec="seconds"),
            "kind": kind,
            "type": record.type_name,
            "key": record.key,
            "version": record.version,
            **(extra or {}),
        }
        self.log.append(entry)
        self.log = self.log[-200:]
        log.info("%s %s/%s v%d %s", kind, record.type_name, record.key, record.version, extra or "")

    def _apply(self, record: ObjectRecord) -> None:
        """Keep the open-orders view consistent whatever order deliveries arrive in."""
        if record.type_name != ORDER:
            return
        current = self.open_orders.get(record.key)
        if current is not None and current.version >= record.version:
            return  # stale or duplicate delivery; the version we hold is newer
        if record.data.get("status") in ("open", "partial"):
            self.open_orders[record.key] = record
        else:
            self.open_orders.pop(record.key, None)

    async def on_insert(self, record: ObjectRecord) -> None:
        self._apply(record)
        self._note("insert", record)

    async def on_update(self, record: ObjectRecord, changed_fields: list[str]) -> None:
        self._apply(record)
        self._note("update", record, {"changed": changed_fields})
        if record.type_name == ORDER and "status" in changed_fields:
            self.vip_activity.append({"order": record.key, "status": record.data["status"]})

    async def on_delete(self, record: ObjectRecord) -> None:
        self.open_orders.pop(record.key, None)
        self._note("delete", record)

    async def on_error(self, error: Exception) -> None:
        self.errors.append(repr(error))
        log.error("callback error: %r", error)


desk = Desk()
client = DBPersistClient(
    **config.client_kwargs(), subscriber_name=config.SUBSCRIBER_NAME, webhook_url=config.WEBHOOK_URL
)


async def _heartbeat() -> None:
    while True:
        await asyncio.sleep(30)
        try:
            await client.heartbeat()
        except Exception as exc:  # noqa: BLE001
            log.warning("heartbeat failed: %r", exc)


async def start_subscriptions() -> None:
    """Register, load current state, and subscribe. Safe to call once per process."""
    await ensure_types(client)
    await client.register_subscriber()

    # 1. Everything that is currently open, plus every future change to open/partial orders.
    open_filter = {"field": "status", "op": "in", "value": ["open", "partial"]}
    rows, _ = await client.active_select(ORDER, open_filter, desk)
    for r in rows:
        desk._apply(r)
    log.info("loaded %d open orders", len(rows))

    # 2. Orders that *leave* the open set must reach us too, otherwise the view never shrinks.
    await client.subscribe(
        ORDER, {"field": "status", "op": "in", "value": ["filled", "cancelled"]}, callbacks=desk
    )

    # 3. Any change to a VIP customer's record, by key, regardless of filters.
    vips = await client.select(CUSTOMER, {"field": "tier", "op": "eq", "value": "vip"})
    if vips:
        await client.subscribe(CUSTOMER, keys=[v.key for v in vips], callbacks=desk)
        log.info("watching %d VIP customers", len(vips))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await start_subscriptions()
    hb = asyncio.create_task(_heartbeat())
    try:
        yield
    finally:
        hb.cancel()
        try:
            await client.unregister_subscriber()
        finally:
            await client.aclose()


app = FastAPI(title="order desk dashboard", lifespan=lifespan)
app.include_router(client.receiver(), prefix="/dbpersist")  # type: ignore[attr-defined]


@app.get("/")
async def dashboard() -> dict:
    return {
        "subscriber": str(client.subscriber_id),
        "open_orders": {
            k: {"version": r.version, **r.data} for k, r in sorted(desk.open_orders.items())
        },
        "status_changes": desk.vip_activity[-20:],
        "recent": desk.log[-20:],
        "errors": desk.errors,
    }


@app.get("/orders/{order_id}")
async def order_detail(order_id: str) -> dict:
    """Read-through to persistence with the customer expanded."""
    rec = await client.get_by_key(ORDER, order_id, expand=["customer"])
    return {"version": rec.version, **rec.data}
