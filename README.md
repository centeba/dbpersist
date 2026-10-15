# dbpersist

Typed, versioned object persistence with change notification, as two small services and an SDK.

| Component | What it is |
|---|---|
| **`packages/persistence`** | FastAPI + SQLAlchemy 2 (asyncpg) + Pydantic over Postgres JSONB: object types, versioned objects with optimistic locking, criteria queries, atomic transactions, and **inbound sources** that accept event notifications from outside systems through a durable, ordered, retrying queue |
| **`packages/pubsub`** | FastAPI API + Celery workers on Redis: subscriptions by filter or key, event matching, HMAC-signed webhook delivery with retries and replay. Optional: persistence runs without it when `PUBSUB_ENABLED=false` |
| **`packages/sdk`** | Async httpx client with `on_insert / on_update / on_delete` callbacks, nested transactions, and a FastAPI webhook receiver |
| **`packages/common`** | JSON filter DSL (one evaluator for SQL and for in-memory matching), event model, signing, API-key auth, logging and OpenTelemetry setup |

Documentation: [Architecture](docs/ARCHITECTURE.md) · [Developer guide](docs/DEVELOPER_GUIDE.md) · [Test cases](docs/TEST_CASES.md) · [Example application](examples/order-desk/README.md)

## How it fits together

1. A client registers an **object type** (name, key fields, optional JSON schema) and writes objects through the persistence API. Each object has a derived key, a `version`, and JSONB `data`.
2. Every committed insert, update or delete is published as an **event** to Redis after the commit (when pub/sub is enabled, the default).
3. The pub/sub worker matches each event against **subscriptions** (a filter tree or a list of keys) and POSTs a signed webhook to each matching **subscriber**, retrying with backoff.
4. The SDK's receiver verifies the signature and calls your callbacks; `active_select` gives you the current rows and every change after them.
5. Events from **outside systems** enter the same pipeline through an **inbound source**: the external publisher POSTs its own notifications to `/v1/inbound/{source}`, they are queued durably, mapped onto local types and applied as ordinary writes, so steps 2 to 4 happen for them too. Sources can also pull a publisher's feed.

Persistence is usable on its own: with `PUBSUB_ENABLED=false` it needs only Postgres, keeps every write, query, transaction and inbound-source feature, and publishes no events.

## Quick start

Host ports are offset to avoid clashing with other local stacks: Postgres **5442**, Redis **6389**, persistence **8101**, pub/sub **8102**.

Everything in Docker:

```bash
docker compose up -d --build
```

Or infrastructure in Docker and services from source:

```bash
docker compose up -d postgres redis
uv sync
uv run --package dbpersist-persistence alembic -c packages/persistence/alembic.ini upgrade head
uv run --package dbpersist-pubsub alembic -c packages/pubsub/alembic.ini upgrade head
```

then in three terminals:

```bash
uv run --package dbpersist-persistence uvicorn dbpersist_persistence.main:app --port 8101 --workers 2
```
```bash
uv run --package dbpersist-pubsub uvicorn dbpersist_pubsub.main:app --port 8102
```
```bash
uv run --package dbpersist-pubsub celery -A dbpersist_pubsub.celery_app worker -Q events,deliveries -c 8 -B --loglevel=info
```
On Windows add `--pool=threads` to the Celery command.

Check: `curl localhost:8101/healthz` and `curl localhost:8102/healthz` return `{"status":"ok", ...}`.

## Using the SDK

```python
from fastapi import FastAPI
from dbpersist_sdk import DBPersistClient, BaseCallbacks

class TradeWatcher(BaseCallbacks):
    async def on_insert(self, record):
        print("new", record.key, record.data)
    async def on_update(self, record, changed_fields):
        print("changed", record.key, changed_fields, "v", record.version)
    async def on_delete(self, record):
        print("gone", record.key)

app = FastAPI()
client = DBPersistClient(
    "http://localhost:8101", "http://localhost:8102", "dev-api-key",
    subscriber_name="my-app", webhook_url="http://my-host:9100/dbpersist/webhook",
)
app.include_router(client.receiver(), prefix="/dbpersist")   # run this app with uvicorn on port 9100

async def main() -> None:
    await client.register_subscriber()
    await client.register_type("Trade", ["trade_id"], {"type": "object", "required": ["trade_id", "qty"]})

    rows, handle = await client.active_select(
        "Trade", {"field": "qty", "op": "gt", "value": 100}, TradeWatcher()
    )

    t = await client.insert("Trade", {"trade_id": "T1", "qty": 150})
    await client.update("Trade", t.id, t.version, {"qty": 160}, mode="merge")   # on_update(["qty"])

    async with client.transaction() as tx:                                       # one atomic commit
        tx.insert("Trade", {"trade_id": "T2", "qty": 200})
        tx.delete("Trade", t.id, 2)
```

## API summary

All endpoints take `X-API-Key` (default `dev-api-key`; set `API_KEY`). Full details and semantics are in the [developer guide](docs/DEVELOPER_GUIDE.md).

**Persistence** (`/v1`)

| Method and path | Purpose |
|---|---|
| `POST /types`, `GET /types`, `GET /types/{name}` | register and read object types |
| `POST /objects/{type}` | insert (201, 409 on duplicate key) |
| `GET /objects/{type}/{id}`, `GET /objects/{type}/by-key/{k1}/{k2}` | read; `?expand=field` inlines `x-ref` references |
| `PUT /objects/{type}/{id}` `{version, data}` | replace the document |
| `PATCH /objects/{type}/{id}` `{version, data}` | merge into the document |
| `DELETE /objects/{type}/{id}?version=` | soft delete |
| `POST /objects/{type}/query` `{filter, order_by, limit, offset \| cursor, expand}` | criteria query, keyset paging |
| `POST /objects/{type}/count` `{filter}` | count |
| `POST /transactions` `{ops: [...]}` | atomic batch of insert / update / delete |
| `POST /publish` | emit an event without persisting |
| `POST /inbound/{source}` | ingress for an external publisher, authenticated by the source's own scheme; stores the message and answers 202 |
| `POST/GET/PUT/DELETE /inbound-sources[/{name}]` | manage inbound sources (auth, mapping, pull); `POST .../test` dry-runs a payload; `GET .../messages`, `POST .../messages/retry`, `POST .../pull`, `GET .../receipts` |
| `GET /health`, `GET /healthz` | liveness, readiness (database) |

Version mismatches return `409 {"code": "version_conflict", "current_version": n}`.

**Pub/sub** (`/v1`)

| Method and path | Purpose |
|---|---|
| `POST /subscribers` `{name, webhook_url}` | register; returns the HMAC `secret` once |
| `DELETE /subscribers/{id}`, `POST /subscribers/{id}/heartbeat` | remove; keep alive |
| `POST /subscribers/{id}/subscriptions` `{type, filter}` or `{type, keys, by: key\|id}` | subscribe |
| `GET /subscribers/{id}/subscriptions`, `DELETE /subscriptions/{id}` | list; unsubscribe |
| `POST /publish` | accept an event from an external producer |
| `POST /subscribers/{id}/replay?since=` | re-queue undelivered events |
| `GET /admin/clients`, `GET /admin/clients/{id}/criteria`, `GET /admin/stats` | operations views |
| `POST /admin/remove-idle`, `PUT /admin/log-level` | housekeeping |
| `GET /health`, `GET /healthz` | liveness, readiness (database and broker) |

Webhook requests carry `X-DBPersist-Signature: sha256=HMAC(secret, "<timestamp>.<body>")`, `X-DBPersist-Timestamp` (5-minute window) and `X-DBPersist-Event-Id`. The body is `{"event": {...}, "subscription_ids": [...], "delivery_id": n}`. Failed deliveries retry with exponential backoff up to 8 times, are then marked `failed`, and can be replayed.

## Inbound sources

Events from outside systems can be brought in as well, independently of whether pub/sub is deployed: register an **inbound source** with the sender's authentication (shared secret, bearer, basic, HMAC with configurable header and signed payload, or dbpersist's own scheme) and a mapping from its envelope to local types, keys, operations and payload, then point the sender at `POST /v1/inbound/{source}`. Messages are stored before they are acknowledged, processed one at a time per source in arrival order with retries and quarantine, deduplicated on the sender's event id, and can also be **pulled** from the publisher's feed on an interval with a stored cursor. Details in the [developer guide](docs/DEVELOPER_GUIDE.md#16-inbound-sources); the [order-desk example](examples/order-desk/README.md#bringing-in-an-external-publishers-events) shows a publisher profile kept as configuration in the consuming application, with register, dry-run and simulate commands.

## Running without pub/sub

If you only need the persistence API, set `PUBSUB_ENABLED=false` on the persistence service and run just Postgres and persistence. Nothing else changes: writes, queries, transactions, publish-only calls and inbound sources all behave the same, change events are counted and dropped instead of being sent to Redis, and the pub/sub containers, Redis and the SDK's subscriber methods are simply not used. Turning it back on later requires no data migration; events start flowing from that point.

```bash
PUBSUB_ENABLED=false uv run --package dbpersist-persistence uvicorn dbpersist_persistence.main:app --port 8101
```

With Docker, the `docker-compose.persistence-only.yml` overlay sets the flag and drops the Redis dependency:

```bash
docker compose -f docker-compose.yml -f docker-compose.persistence-only.yml up -d postgres persistence
```

## Filter DSL

```json
{"and": [
  {"field": "qty", "op": "gt", "value": 100},
  {"or": [{"field": "book", "op": "like", "value": "ALP%"}, {"field": "book", "op": "is_null"}]}
]}
```

Ops: `eq ne lt le gt ge like not_like is_null is_not_null in`. Fields are dotted paths into the object's data (`leg.qty`); `id`, `key`, `version`, `created_at`, `updated_at` address the object's own columns. Datetimes are ISO strings with `"value_type": "datetime"`. The same tree drives SQL queries and in-memory event matching.

## Observability

Services log structured JSON with trace ids and export traces, metrics and logs over OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. To ship to the observability platform (OTLP collector, Loki, Tempo, Prometheus, Grafana) running from the `sentinelbuild_observability` compose project on the same host:

```bash
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d --build
```

Metric names, log fields and Grafana queries are listed in the [developer guide](docs/DEVELOPER_GUIDE.md#8-logging-and-observability).

## Tests

```bash
uv run pytest packages            # unit + API tests (Postgres container required)
uv run pytest tests/integration   # scenarios against the live stack (skips if it is not running)
uv run python scripts/smoke_e2e.py
uv run pytest                     # all of the above
```

When the Celery worker runs in Docker, webhooks reach the host through `host.docker.internal`. The integration harness defaults to that; for the smoke script set:

```bash
WEBHOOK_URL=http://host.docker.internal:9100/dbpersist/webhook RECEIVER_HOST=0.0.0.0 uv run python scripts/smoke_e2e.py
```

The scenario catalogue is in [docs/TEST_CASES.md](docs/TEST_CASES.md).

## Notes

- Events are enqueued **after** the database commit. If the enqueue itself fails the write stands and the request returns 502; replay through the pub/sub API is the recovery path.
- One pub/sub deployment serves all subscribers; there is no server-to-server forwarding.
