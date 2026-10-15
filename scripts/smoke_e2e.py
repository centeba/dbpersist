"""End-to-end smoke test: real services, real Postgres/Redis, real webhook round trip.

Prerequisites (see README): Postgres + Redis up, both migrations applied, persistence API,
pubsub API and a Celery worker running. Then:

    uv run python scripts/smoke_e2e.py

Env overrides: PERSIST_URL, PUBSUB_URL, API_KEY, RECEIVER_HOST, RECEIVER_PORT, WEBHOOK_URL.
Exit code 0 on success, 1 on failure.
"""

import asyncio
import os
import sys
import time
import uuid

import uvicorn
from fastapi import FastAPI

from dbpersist_sdk import BaseCallbacks, DBPersistClient, ObjectRecord

PERSIST_URL = os.environ.get("PERSIST_URL", "http://localhost:8101")
PUBSUB_URL = os.environ.get("PUBSUB_URL", "http://localhost:8102")
API_KEY = os.environ.get("API_KEY", "dev-api-key")
RECEIVER_HOST = os.environ.get("RECEIVER_HOST", "127.0.0.1")
RECEIVER_PORT = int(os.environ.get("RECEIVER_PORT", "9100"))
# When pubsub-worker runs inside docker, the receiver must be reachable as host.docker.internal.
WEBHOOK_URL = os.environ.get(
    "WEBHOOK_URL", f"http://{RECEIVER_HOST}:{RECEIVER_PORT}/dbpersist/webhook"
)
TIMEOUT = float(os.environ.get("SMOKE_TIMEOUT", "15"))


class Collector(BaseCallbacks):
    def __init__(self) -> None:
        self.inserts: list[ObjectRecord] = []
        self.updates: list[tuple[ObjectRecord, list[str]]] = []
        self.deletes: list[ObjectRecord] = []
        self.errors: list[Exception] = []
        self.changed = asyncio.Event()

    async def on_insert(self, record: ObjectRecord) -> None:
        self.inserts.append(record)
        self.changed.set()

    async def on_update(self, record: ObjectRecord, changed_fields: list[str]) -> None:
        self.updates.append((record, changed_fields))
        self.changed.set()

    async def on_delete(self, record: ObjectRecord) -> None:
        self.deletes.append(record)
        self.changed.set()

    async def on_error(self, error: Exception) -> None:
        self.errors.append(error)
        self.changed.set()


async def wait_for(predicate, cb: Collector, what: str) -> None:
    deadline = time.monotonic() + TIMEOUT
    while not predicate():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"timed out waiting for {what}")
        cb.changed.clear()
        try:
            await asyncio.wait_for(cb.changed.wait(), timeout=remaining)
        except TimeoutError:
            pass


async def main() -> int:
    run = uuid.uuid4().hex[:8]
    type_name = f"SmokeTrade_{run}"
    cb = Collector()

    client = DBPersistClient(
        PERSIST_URL, PUBSUB_URL, API_KEY, subscriber_name=f"smoke-{run}", webhook_url=WEBHOOK_URL
    )
    app = FastAPI()
    app.include_router(client.receiver(), prefix="/dbpersist")  # type: ignore[attr-defined]
    server = uvicorn.Server(
        uvicorn.Config(app, host=RECEIVER_HOST, port=RECEIVER_PORT, log_level="warning")
    )
    server_task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)

    try:
        async with client:
            print(f"[1] register subscriber -> {WEBHOOK_URL}")
            await client.register_subscriber()

            print("[2] register type and subscribe to qty > 100")
            await client.register_type(
                type_name, ["trade_id"], {"type": "object", "required": ["trade_id", "qty"]}
            )
            await client.subscribe(
                type_name, {"field": "qty", "op": "gt", "value": 100}, callbacks=cb
            )

            print("[3] insert T1 (qty 150, should match) and T2 (qty 50, should not)")
            t1 = await client.insert(type_name, {"trade_id": "T1", "qty": 150})
            await client.insert(type_name, {"trade_id": "T2", "qty": 50})
            await wait_for(lambda: len(cb.inserts) >= 1, cb, "insert webhook")
            await asyncio.sleep(1.0)  # give a wrong T2 delivery time to show up
            assert [r.key for r in cb.inserts] == ["T1"], cb.inserts

            print("[4] update T1 qty -> 160, expect on_update with changed_fields == ['qty']")
            await client.update(type_name, t1.id, t1.version, {"qty": 160}, mode="merge")
            await wait_for(lambda: len(cb.updates) >= 1, cb, "update webhook")
            rec, changed = cb.updates[0]
            assert rec.key == "T1" and rec.version == 2 and changed == ["qty"], (rec, changed)

            print("[5] select via criteria and count")
            rows = await client.select(type_name, {"field": "qty", "op": "ge", "value": 100})
            assert [r.key for r in rows] == ["T1"], rows
            assert await client.count(type_name) == 2

            print("[6] delete T1, expect on_delete")
            await client.delete(type_name, t1.id, 2)
            await wait_for(lambda: len(cb.deletes) >= 1, cb, "delete webhook")
            assert cb.deletes[0].key == "T1"
            assert not cb.errors, cb.errors

            await client.unregister_subscriber()
        print("SMOKE OK")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"SMOKE FAILED: {exc!r}")
        return 1
    finally:
        server.should_exit = True
        await server_task


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
