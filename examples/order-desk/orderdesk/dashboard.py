"""The subscriber side: a FastAPI service that keeps a live view of open orders.

What it shows:

- ``active_select`` to load the current open orders *and* subscribe to changes that match the
  same filter, in one call;
- a key subscription for the orders of VIP customers, so the desk sees every change to those
  regardless of status;
- the SDK webhook receiver mounted into the app, verifying HMAC signatures for us;
- callbacks that reconcile by ``version`` so late or duplicate deliveries never regress state;
- a heartbeat task so the pub/sub service knows this subscriber is alive;
- a server-sent events stream (``/events``) that pushes every notification to the React UI
  the moment the webhook lands, plus small action and admin-proxy endpoints the UI uses.

Run with ``uv run uvicorn orderdesk.dashboard:app --host 0.0.0.0 --port 9200``. With the UI
built (``cd ui && npm run build``) it is served at http://localhost:9200/app/.
"""

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from dbpersist_sdk import (
    BaseCallbacks,
    DBPersistClient,
    DBPersistError,
    NotFound,
    ObjectRecord,
    VersionConflict,
)
from orderdesk import cli, config
from orderdesk.domain import CUSTOMER, ORDER, ensure_types

log = logging.getLogger("orderdesk.dashboard")


class Desk(BaseCallbacks):
    """In-memory state fed by webhooks. One instance serves every subscription."""

    def __init__(self) -> None:
        self.open_orders: dict[str, ObjectRecord] = {}
        self.vip_activity: list[dict] = []
        self.log: list[dict] = []
        self.errors: list[str] = []
        self.listeners: set[asyncio.Queue[dict]] = set()

    def _broadcast(self, entry: dict) -> None:
        for q in list(self.listeners):
            try:
                q.put_nowait(entry)
            except asyncio.QueueFull:
                self.listeners.discard(q)

    def _note(self, kind: str, record: ObjectRecord, extra: dict | None = None) -> None:
        entry = {
            "at": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "kind": kind,
            "type": record.type_name,
            "key": record.key,
            "version": record.version,
            "data": record.data,
            **(extra or {}),
        }
        self.log.append(entry)
        self.log = self.log[-200:]
        log.info("%s %s/%s v%d %s", kind, record.type_name, record.key, record.version, extra or "")
        self._broadcast({"event": "notification", **entry})

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
        self._broadcast({"event": "error", "message": repr(error)})

    def snapshot(self) -> dict:
        return {
            "subscriber": str(client.subscriber_id),
            "subscriber_name": client.subscriber_name,
            "webhook_url": client.webhook_url,
            "open_orders": {
                k: {"version": r.version, **r.data} for k, r in sorted(self.open_orders.items())
            },
            "status_changes": self.vip_activity[-20:],
            "recent": self.log[-50:],
            "errors": self.errors,
        }


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


async def _register_or_take_over() -> None:
    """Register; if a previous instance left a subscriber with our name, replace it.

    A subscriber's secret is only returned at registration, so a restarted dashboard cannot
    reuse the old row. Deleting it cascades its subscriptions and pending deliveries, which is
    the right outcome for a single-instance dashboard that rebuilds its view on startup.
    """
    try:
        await client.register_subscriber()
        return
    except DBPersistError as exc:
        if exc.status != 409:
            raise
    async with _admin() as a:
        r = await a.get("/admin/clients")
        r.raise_for_status()
        stale = [c for c in r.json() if c["name"] == client.subscriber_name]
        for c in stale:
            await a.delete(f"/subscribers/{c['id']}")
            log.warning("replaced stale subscriber %s (%s)", c["name"], c["id"])
    await client.register_subscriber()


async def start_subscriptions() -> None:
    """Register, load current state, and subscribe. Safe to call once per process."""
    await ensure_types(client)
    await _register_or_take_over()

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
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)  # the Vite dev server runs on another port
app.include_router(client.receiver(), prefix="/dbpersist")  # type: ignore[attr-defined]


# ---- views -------------------------------------------------------------------------------


@app.get("/api/state")
async def state() -> dict:
    return desk.snapshot()


@app.get("/api/orders/{order_id}")
async def order_detail(order_id: str) -> dict:
    """Read-through to persistence with the customer expanded."""
    try:
        rec = await client.get_by_key(ORDER, order_id, expand=["customer"])
    except NotFound as exc:
        raise HTTPException(404, str(exc)) from exc
    return {"id": str(rec.id), "version": rec.version, **rec.data}


@app.get("/api/events")
async def events() -> StreamingResponse:
    """Server-sent events: one message per webhook notification, as it arrives."""
    q: asyncio.Queue[dict] = asyncio.Queue(maxsize=500)
    desk.listeners.add(q)

    async def gen() -> AsyncIterator[str]:
        try:
            yield "event: snapshot\ndata: " + json.dumps(desk.snapshot(), default=str) + "\n\n"
            while True:
                try:
                    entry = await asyncio.wait_for(q.get(), timeout=15)
                except TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                yield f"event: {entry['event']}\ndata: {json.dumps(entry, default=str)}\n\n"
        finally:
            desk.listeners.discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})


# ---- pub/sub admin, proxied so the browser never holds the API key -----------------------


def _admin() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=f"{config.PUBSUB_URL}/v1", headers={"X-API-Key": config.API_KEY}, timeout=10
    )


@app.get("/api/pubsub/stats")
async def pubsub_stats() -> dict:
    async with _admin() as a:
        r = await a.get("/admin/stats")
        r.raise_for_status()
        return r.json()


@app.get("/api/pubsub/clients")
async def pubsub_clients() -> list[dict]:
    async with _admin() as a:
        r = await a.get("/admin/clients")
        r.raise_for_status()
        return r.json()


@app.get("/api/pubsub/subscriptions")
async def pubsub_subscriptions() -> list[dict]:
    async with _admin() as a:
        r = await a.get(f"/subscribers/{client.subscriber_id}/subscriptions")
        r.raise_for_status()
        return r.json()


# ---- actions: generate traffic from the UI and watch it come back as webhooks -----------


class SeedIn(BaseModel):
    orders: int = 5


class FillIn(BaseModel):
    order_id: str
    qty: int


class CancelIn(BaseModel):
    order_id: str


@app.post("/api/actions/seed")
async def action_seed(body: SeedIn) -> dict:
    async with DBPersistClient(**config.client_kwargs()) as c:
        await cli.seed(c, max(1, min(body.orders, 50)))
    return {"ok": True}


@app.post("/api/actions/fill")
async def action_fill(body: FillIn) -> dict:
    async with DBPersistClient(**config.client_kwargs()) as c:
        for _ in range(5):
            rec = await c.get_by_key(ORDER, body.order_id)
            filled = rec.data.get("filled", 0) + body.qty
            if filled > rec.data["qty"]:
                raise HTTPException(422, f"fill of {body.qty} exceeds remaining quantity")
            status = "filled" if filled == rec.data["qty"] else "partial"
            try:
                new = await c.update(
                    ORDER, rec.id, rec.version,
                    {"filled": filled, "status": status, "notes": [*rec.data.get("notes", []), f"fill {body.qty}"]},
                    mode="merge",
                )
                return {"version": new.version, "status": new.data["status"], "filled": new.data["filled"]}
            except VersionConflict:
                continue
    raise HTTPException(409, "too many concurrent updates; try again")


@app.post("/api/actions/cancel")
async def action_cancel(body: CancelIn) -> dict:
    async with DBPersistClient(**config.client_kwargs()) as c:
        rec = await c.get_by_key(ORDER, body.order_id)
        if rec.data["status"] in ("filled", "cancelled"):
            raise HTTPException(409, f"already {rec.data['status']}")
        new = await c.update(ORDER, rec.id, rec.version, {"status": "cancelled"}, mode="merge")
        return {"version": new.version, "status": new.data["status"]}


# ---- the built React UI, when present ----------------------------------------------------

_UI_DIST = Path(__file__).resolve().parent.parent / "ui" / "dist"
if _UI_DIST.is_dir():
    app.mount("/app", StaticFiles(directory=_UI_DIST, html=True), name="ui")

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse("/app/")
else:

    @app.get("/")
    async def root_json() -> dict:
        return desk.snapshot()
