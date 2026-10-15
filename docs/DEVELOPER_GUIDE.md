# dbpersist developer guide

This guide is for people changing dbpersist. The [README](../README.md) covers running it and the API; [TEST_CASES.md](TEST_CASES.md) catalogues the tests.

Contents

1. [Architecture](#1-architecture)
2. [Repository layout](#2-repository-layout)
3. [Local setup](#3-local-setup)
4. [Persistence service internals](#4-persistence-service-internals)
5. [Pub/sub service internals](#5-pubsub-service-internals)
6. [Filter DSL](#6-filter-dsl)
7. [SDK](#7-sdk)
8. [Logging and observability](#8-logging-and-observability)
9. [Configuration reference](#9-configuration-reference)
10. [Database migrations](#10-database-migrations)
11. [Testing](#11-testing)
12. [Coding conventions](#12-coding-conventions)
13. [Common tasks](#13-common-tasks)
14. [Troubleshooting](#14-troubleshooting)
15. [Performance](#15-performance)

## 1. Architecture

```
 client app ──SDK (httpx)──▶ persistence API ──commit──▶ Postgres (persist)
      ▲                            │
      │                            └── after commit: Celery task "pubsub.publish_event" ──▶ Redis
      │                                                                                     │
      │   signed webhook POST ◀── pubsub worker: match subscriptions, record delivery ◀──────┘
      │                                   │
      └── SDK receiver router             └── Postgres (pubsub): subscribers, subscriptions, deliveries
          verifies HMAC, dispatches                  ▲
          on_insert / on_update / on_delete          │
                                            pubsub API: register, subscribe, replay, admin
```

- **Persistence** owns object types and versioned JSONB objects. Every committed change becomes an `ObjectEvent`.
- **Pub/sub** owns subscribers and subscriptions. It never touches the persistence database; it only consumes events and pushes webhooks.
- The two share **Redis** (Celery broker) and the **`common`** package (filter DSL, event model, auth, telemetry). They use separate Postgres databases in one instance.
- Trace context flows end to end: SDK → persistence (HTTP) → Celery headers → worker → webhook (`traceparent`) → receiver.

Design decisions worth knowing:

| Decision | Why |
|---|---|
| One JSONB `objects` table with a type registry, not a table per type | Types are registered at runtime without DDL; one query path and one filter translator cover every type; the GIN index serves equality filters. |
| `version` column for optimistic locking | Writers send the version they read; a mismatch is a 409 with the current version, never a silent overwrite. |
| Soft delete (`deleted_at`) | Delete events carry the last document image, and a key can be reused after deletion (partial unique index). |
| Events enqueued after commit, by task name only | Persistence has no import dependency on pubsub; the Redis queue survives pubsub API downtime. |
| Celery tasks use a sync SQLAlchemy engine | Prefork workers and asyncio event loops do not mix; the FastAPI side uses asyncpg. |
| Per-subscriber dedupe | A subscriber with several matching subscriptions receives one webhook listing all matching subscription ids. |
| HMAC-signed webhooks with a timestamp | Receivers can authenticate deliveries without TLS client certs; the 5-minute window limits replay. |

## 2. Repository layout

```
pyproject.toml                   uv workspace, pytest/ruff/pyright config
docker-compose.yml               postgres, redis, persistence, pubsub-api, pubsub-worker, pubsub-beat
docker-compose.observability.yml overlay: OTLP env + join the observability network
Dockerfile                       one image for all Python services (uv sync --frozen)
docker/init-db.sql               creates persist, pubsub and the *_test databases
packages/
  common/     dbpersist_common: filters/ (models, sqla, evaluate, paths), events, keys, signing, auth, settings, telemetry
  persistence/dbpersist_persistence: main, settings, db, models, schemas, routers/, services/, celery_app, alembic/
  pubsub/     dbpersist_pubsub: main, settings, db, models, schemas, routers/, tasks/, matching, stats, celery_app, alembic/
  sdk/        dbpersist_sdk: client (DBPersistClient, Transaction), receiver, models, exceptions
tests/integration/               scenario tests against the live stack
scripts/smoke_e2e.py             minimal end-to-end check
docs/                            this guide, TEST_CASES.md
```

Each package has `src/<module>` and `tests/`. The workspace shares one `.venv` and one `uv.lock`.

## 3. Local setup

Prerequisites: Docker Desktop, [uv](https://docs.astral.sh/uv/). Python 3.14 is pinned in `.python-version`; uv installs it if needed.

```bash
docker compose up -d postgres redis
uv sync
uv run --package dbpersist-persistence alembic -c packages/persistence/alembic.ini upgrade head
uv run --package dbpersist-pubsub alembic -c packages/pubsub/alembic.ini upgrade head
uv run pytest packages                      # unit + API tests, ~20 s
```

Run the services from source (three terminals) as shown in the README, or everything in Docker with `docker compose up -d --build`. Host ports: Postgres 5442, Redis 6389, persistence 8101, pubsub 8102. They are offset on purpose so dbpersist can coexist with other local stacks; change them in `docker-compose.yml` and `.env` if you need the defaults.

`.env.example` lists every variable; copy it to `.env` for local overrides (pydantic-settings reads `.env` from the current directory).

## 4. Persistence service internals

Request flow for a write: router → `services.objects` → SQLAlchemy session → commit → `services.events.event_for` → `EventPublisher.publish`.

- **`models.py`**: `ObjectType(name, key_fields, json_schema)` and `Object(id, type_name, key, version, data, deleted_at, created_at, updated_at)` with the partial unique index on live keys, a GIN index on `data`, and a `(type_name, updated_at, id)` index for keyset paging.
- **`services/objects.py`**: key derivation (`common.keys.build_key`, key field values joined with the ASCII unit separator), JSON-schema validation, `deep_merge` for merge updates, `changed_fields` (top-level keys whose value differs), row locking with `SELECT ... FOR UPDATE`, version checks, soft delete, `expand` for `x-ref` fields, and `with_retry` for serialization/deadlock errors (SQLSTATE 40001/40P01, three attempts).
- **`services/query.py`**: builds the SELECT from a `QueryIn`: base predicate (type, not deleted) + `common.filters.to_sqla`, ordering by column or JSONB value (numbers sort numerically), offset paging, or keyset cursor paging on `(updated_at, id)` encoded as base64 JSON. A `next_cursor` is returned only for cursor-style queries that filled the page.
- **`routers/transactions.py`**: applies a list of insert/update/delete ops in one session and commits once; events for all ops share a `transaction_id` and are published only after commit.
- **`routers/publish.py`**: builds an event without touching the database (for callers that persist elsewhere).
- **`services/events.py`**: `EventPublisher` protocol. `CeleryEventPublisher` calls `send_task("pubsub.publish_event")`; `RecordingPublisher` is the test double. Every published event increments the `dbpersist.persistence.events` counter and writes a structured log line.
- **Errors**: services raise `NotFound`, `DuplicateKey`, `VersionConflict`, `InvalidData`; `main.py` maps them to 404/409/409/422 with a `code` field the SDK uses.

Adding an endpoint: add the schema to `schemas.py`, the logic to a `services/` module (pure where possible), the route to a router, and a test in `tests/`. Keep routers thin.

## 5. Pub/sub service internals

- **`models.py`**: `Subscriber(name, webhook_url, secret, last_seen_at, last_delivery_ok_at)`, `Subscription(type_name, filter | keys, by)` with a CHECK constraint that exactly one of filter/keys is set, and `Delivery(event_id, subscriber_id, subscription_ids, event, status, attempts, last_error, delivered_at)`.
- **`matching.py`**: `subscription_matches` evaluates a filter with `common.filters.matches` against `event.data` plus `key`, `version` and `id`; key subscriptions compare `event.key` (or `object_id` when `by == "id"`). `select_matching` groups matching subscription ids per subscriber. Parsed filters are cached per subscription id (`parsed_filter`): parsing costs ~20 µs, evaluating ~1 µs, and subscriptions are immutable, so the cache never goes stale.
- **`tasks/match.py`**: task `pubsub.publish_event` → `fan_out` validates the event, loads subscriptions for the type and inserts one pending `Delivery` per subscriber (two autocommit statements; `INSERT … ON CONFLICT DO NOTHING` on the unique `(event_id, subscriber_id)` index, so a message redelivered after a worker died mid-task reuses the rows it already created and only hands out the undelivered ones). When exactly one subscriber matched and `DELIVER_INLINE` is on (default), the first webhook attempt is made inline and the second Celery hop is skipped; on failure the delivery is handed to `deliver_webhook` with `retries=1`, so the retry schedule and attempt count are the same as before. With several subscribers, one `deliver_webhook` task is enqueued per delivery.
- **`tasks/deliver.py`**: `attempt_delivery` loads the delivery joined to its subscriber (one SELECT), builds the signed request (`common.signing`), POSTs with a 10 s timeout through one keep-alive `httpx.Client` per worker process (`shared_client`), records attempts/duration metrics and a log line, and writes the outcome with one UPDATE on the autocommit engine. The subscriber's `last_delivery_ok_at` is refreshed at most once per second, because every delivery to a subscriber otherwise locks the same row. The Celery task retries on `httpx.HTTPError` with exponential backoff and jitter (`backoff_seconds`), up to `WEBHOOK_MAX_RETRIES`; after that the delivery is marked `failed` and can be replayed via `POST /subscribers/{id}/replay`.
- **`tasks/cleanup.py`**: beat jobs remove subscribers idle beyond `IDLE_SUBSCRIBER_MINUTES` (no heartbeat and no successful delivery) and prune deliveries older than `DELIVERY_RETENTION_DAYS`.
- **`stats.py`**: Redis counters for the admin `/stats` endpoint, mirrored to OpenTelemetry counters.
- **`routers/admin.py`**: clients with subscription/pending/failed counts, criteria by client, stats, manual idle cleanup, runtime log level.
- **`celery_app.py`**: broker/queue config (`events`, `deliveries`), beat schedule, a 120 s hard time limit (a timed-out or lost task is requeued, not acked), and worker-process telemetry setup via the `worker_init` / `worker_process_init` signals.
- **`db.py`**: `sync_engine()` (no `pool_pre_ping`: it cost a round trip per checkout, and a stale connection already fails into Celery's retry/requeue path; `pool_recycle` drops connections after 30 minutes) and `autocommit_engine()`, the same pool in AUTOCOMMIT mode for the task hot paths.

Celery on Windows needs `--pool=threads`; the Docker image uses prefork with one child per core (see [§15](#15-performance) for why).

## 6. Filter DSL

Defined once in `common/filters/models.py` and used by both services and the SDK.

```json
{"and": [{"field": "qty", "op": "gt", "value": 100},
         {"or": [{"field": "book", "op": "like", "value": "ALP%"}, {"field": "book", "op": "is_null"}]}]}
```

- Ops: `eq ne lt le gt ge like not_like is_null is_not_null in`. `is_null`/`is_not_null` take no value; `in` takes a non-empty list; everything else a scalar.
- Fields are dotted paths into `data`. `id`, `key`, `version`, `created_at`, `updated_at` address object columns.
- `value_type: "datetime"` marks ISO-8601 strings for datetime comparison; `"bytes"` marks base64.
- **`sqla.py`** translates to SQLAlchemy: numbers compare as `(data #>> path)::numeric`, booleans as `::boolean`, strings as text; `eq` on a scalar also adds a JSONB containment predicate so the GIN index can be used.
- **`evaluate.py`** is the in-memory twin used by pubsub matching. Missing paths count as null; type mismatches evaluate false rather than raising.

The two implementations are kept in lock-step by `packages/persistence/tests/test_query.py::test_db_matches_evaluator`, which runs every filter in its table through Postgres and through `matches()` and requires identical results. When you add an operator, add it to the models, both translators, and that table.

## 7. SDK

`DBPersistClient` wraps both services with one `httpx.AsyncClient` each. Highlights:

- `select_iter` follows `next_cursor` transparently; `select` is a single page.
- `transaction()` returns a `Transaction` context manager that collects ops; nested `begin()` blocks increase a depth counter and the single commit happens when the outermost block exits. An exception inside discards the ops.
- `active_select` subscribes **before** selecting so no change is missed between the two calls.
- `register_subscriber` stores the subscriber id and secret on the client; `use_subscriber(id, secret)` restores them after a restart.
- `client.receiver()` returns a FastAPI router; mount it and point the subscriber's `webhook_url` at it. The receiver verifies the HMAC and timestamp, updates the optional identity cache, and dispatches to the `Callbacks` registered per subscription id, once per distinct callbacks object. A callback that raises is reported through `on_error` and the webhook still returns 200, so pubsub does not retry application bugs.
- Errors map by the response's `code`: `NotFound`, `DuplicateKey`, `VersionConflict` (with `current_version`), `InvalidData`, `Unauthorized`; anything else is `DBPersistError`.

## 8. Logging and observability

All of it lives in `common/telemetry.py` and is switched on by `init_telemetry(service_name, settings)` in each service's `create_app` and in the Celery worker signals.

**Logs.** Structured JSON on stdout by default (`LOG_FORMAT=text` for a human format), one object per line with `ts`, `level`, `logger`, `service`, `message`, any `extra={...}` fields, and `trace_id`/`span_id` when written inside a span. A request-log middleware writes one line per HTTP request (`http_method`, `http_path`, `http_status`, `duration_ms`, `client_ip`), replacing uvicorn's access log; `/health` and `/healthz` are not logged. Domain events log with `event_id`, `type_name`, `op`, `object_key`, `object_version`, `changed_fields`, `transaction_id`; deliveries log with `delivery_id`, `subscriber`, `attempt`, `duration_ms`, `http_status`.

Write logs with fields, not formatted strings:

```python
log.info("delivered", extra={"delivery_id": d.id, "attempt": d.attempts, "duration_ms": ms})
```

**Traces.** FastAPI, SQLAlchemy, httpx and Celery are auto-instrumented. One trace covers SDK call → persistence request → Celery publish → worker match → delivery POST → receiver. Add attributes to the current span for anything worth filtering on (`trace.get_current_span().set_attribute(...)`); never put tenant ids on metric labels.

**Metrics** (OTLP, exported every 15 s; the collector renders them for Prometheus as `<name with dots as underscores>_total`):

| Metric | Attributes | Meaning |
|---|---|---|
| `dbpersist.persistence.events` | `type_name`, `op` | change events emitted |
| `dbpersist.pubsub.events.received` | | events taken off the queue |
| `dbpersist.pubsub.deliveries.created` | | deliveries created by matching |
| `dbpersist.pubsub.deliveries.delivered` | | webhooks acknowledged |
| `dbpersist.pubsub.deliveries.failed` | | deliveries that exhausted retries |
| `dbpersist.pubsub.delivery.attempts` | `outcome` = ok / error | webhook POST attempts |
| `dbpersist.pubsub.delivery.duration` (ms histogram) | `outcome` | webhook round-trip |
| `http.server.*`, `db.client.*` | standard | from the auto-instrumentation |

**Health.** `/health` is liveness. `/healthz` is readiness: persistence checks the database, pubsub checks database and broker, both return 503 with the failing component named.

**Shipping to the observability platform** (the `sentinelbuild_observability` compose project, which exposes an OTLP collector, Loki, Tempo, Prometheus and Grafana):

```bash
# 1. start the platform (its own repo)
docker compose up -d
# 2. start dbpersist joined to its network with OTLP enabled
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d --build
```

The overlay sets `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318`, a distinct `OTEL_SERVICE_NAME` per process (`dbpersist-persistence`, `dbpersist-pubsub`, `dbpersist-pubsub-worker`, `dbpersist-pubsub-beat`), and joins the external `observability_default` network. To have the platform's fleet status probe dbpersist, add to that project's `.env`:

```
OBS_HEALTH_TARGETS={"dbpersist-persistence":"http://persistence:8001","dbpersist-pubsub":"http://pubsub-api:8002"}
```

Then in Grafana (`http://127.0.0.1:3000`): Explore → Loki `{service_name="dbpersist-pubsub-worker"} |= "delivered"`; Explore → Tempo search by service `dbpersist-persistence`; Explore → Prometheus `rate(dbpersist_pubsub_deliveries_delivered_total[5m])`.

Running from source on the host instead of Docker, set `OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318` in `.env`. Leave it empty to disable export entirely; logging still works.

## 9. Configuration reference

All settings are environment variables (or `.env`), read by pydantic-settings; names are case-insensitive.

| Variable | Default | Used by | Meaning |
|---|---|---|---|
| `API_KEY` | `dev-api-key` | both APIs, SDK | value of the `X-API-Key` header |
| `REDIS_URL` | `redis://localhost:6389/0` | both | Celery broker and pubsub stats |
| `PERSIST_DATABASE_URL` | `postgresql+asyncpg://…:5442/persist` | persistence | async DSN |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` | 10 / 10 | persistence | asyncpg pool |
| `PUBSUB_DATABASE_URL` | `postgresql+asyncpg://…:5442/pubsub` | pubsub API | async DSN |
| `PUBSUB_SYNC_DATABASE_URL` | `postgresql+psycopg://…:5442/pubsub` | pubsub worker, alembic | sync DSN |
| `IDLE_SUBSCRIBER_MINUTES` | 60 | pubsub | idle cleanup threshold |
| `DELIVERY_RETENTION_DAYS` | 7 | pubsub | delivery log retention |
| `WEBHOOK_TIMEOUT_SECONDS` | 10 | pubsub | per-POST timeout |
| `WEBHOOK_MAX_RETRIES` | 8 | pubsub | retries before `failed` |
| `WEBHOOK_BACKOFF_MAX_SECONDS` | 600 | pubsub | backoff cap |
| `DELIVER_INLINE` | `true` | pubsub worker | first webhook attempt inside `publish_event` when one subscriber matches |
| `PUBSUB_WORKER_CONCURRENCY` | 16 | docker-compose | prefork children of `pubsub-worker` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | empty (off) | all | collector base URL |
| `OTEL_SERVICE_NAME` | per process | all | overrides the built-in service name |
| `OTEL_SERVICE_VERSION` | `0.1.0` | all | resource attribute |
| `DEPLOYMENT_ENVIRONMENT` | `development` | all | resource attribute |
| `LOG_LEVEL` | `INFO` | all | root log level |
| `LOG_FORMAT` | `json` | all | `json` or `text` |

## 10. Database migrations

Each service owns an Alembic environment: `packages/persistence/alembic` (async env) and `packages/pubsub/alembic` (sync env). The ORM models are the source of truth.

```bash
# after changing models.py
uv run --package dbpersist-persistence alembic -c packages/persistence/alembic.ini revision --autogenerate -m "describe change"
uv run --package dbpersist-persistence alembic -c packages/persistence/alembic.ini upgrade head
```

Review autogenerated revisions: partial indexes, GIN options and CHECK constraints need to be written by hand (see `0001_initial.py` in each package). The Docker services run `alembic upgrade head` before starting. Tests do not use Alembic; they create the schema from the models in the `*_test` databases.

## 11. Testing

| Layer | Command | Needs |
|---|---|---|
| Unit + API | `uv run pytest packages` | the Postgres container (Redis for the pubsub readiness test) |
| Integration scenarios | `uv run pytest tests/integration` | the full stack running; skips itself otherwise |
| Smoke | `uv run python scripts/smoke_e2e.py` | the full stack |
| Everything | `uv run pytest` | |

Conventions: `pytest-asyncio` in auto mode; API tests use `httpx.AsyncClient(transport=ASGITransport(app))`; the persistence tests inject `RecordingPublisher` instead of Celery; pubsub tests run Celery eagerly with `task_eager_propagates=False` so `retry()` re-executes in-process, and replace the webhook client with `httpx.MockTransport` (`dbpersist_pubsub.testing.WebhookSink`). Integration tests create uniquely named types and subscribers, so they can run repeatedly against a shared database. See [TEST_CASES.md](TEST_CASES.md) for the scenario catalogue.

Lint and types:

```bash
uv run ruff format packages tests scripts
uv run ruff check packages tests scripts
uv run pyright packages          # source is clean; tests have a few Optional-narrowing notes
```

## 12. Coding conventions

- Python 3.14 features are fine (PEP 695 generics are used in `with_retry`). Annotate every parameter and return; parameterise containers.
- Pydantic models at every boundary; dicts only for raw JSON payloads (`data`, filters).
- Routers stay thin; logic lives in `services/` (persistence) or `tasks/` + `matching.py` (pubsub) and is unit-testable without HTTP.
- Domain errors are exceptions mapped centrally in `main.py`; the SDK relies on the `code` field.
- Logs are structured (`extra={...}`); no f-string payloads. Metrics have bounded label sets.
- Keep the two filter implementations in parity and covered by the parity test.
- `ruff format` + `ruff check` must pass; line length 100; imports sorted with first-party = the four packages.

## 13. Common tasks

**Add a field to the object table.** Edit `persistence/models.py`, generate and review a migration, extend `schemas.ObjectOut` and the SDK `ObjectRecord`, add it to `query.COLUMNS` if it should be filterable as a column field.

**Add a filter operator.** `common/filters/models.py` (`Op` literal and validation), `sqla.py` (`_compare`), `evaluate.py` (`_eval_clause`), then rows in `common/tests/test_evaluate.py`, `common/tests/test_sqla_compile.py` and the parity table in `persistence/tests/test_query.py`.

**Add an admin endpoint.** Route in `pubsub/routers/admin.py`, schema in `schemas.py`, test in `pubsub/tests/test_api.py`, row in `docs/TEST_CASES.md` if it is user-visible behaviour.

**Change retry policy.** `WEBHOOK_MAX_RETRIES` / `WEBHOOK_BACKOFF_MAX_SECONDS`, or `backoff_seconds` in `tasks/deliver.py`; `test_backoff_grows_and_caps` pins the shape.

**Add a metric.** Create it from `get_meter("dbpersist.<service>")` at module level next to the code that records it; document it in §8.

**Measure pub/sub throughput.** Run `scripts/bench_pubsub.py` inside the Docker network (§15). Compare the `drain rate` line before and after a change; keep the per-task round-trip count (DB statements, Redis commands, Celery hops) from growing, that is what the worker is bound by.

## 14. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `password authentication failed` or `port is already allocated` on startup | Another Postgres/Redis owns the default ports; dbpersist expects 5442/6389. Check `docker ps` and `.env`. |
| Writes succeed but no webhooks arrive | Is a worker running and consuming `events,deliveries`? Check `GET /v1/admin/clients` for `pending` counts and `GET /v1/admin/stats`. In Docker, the receiver must be reachable from the worker container (`host.docker.internal`). |
| Deliveries end up `failed` | The receiver returned non-2xx or timed out for all retries. Fix the receiver, then `POST /v1/subscribers/{id}/replay?since=...`. |
| `MissingGreenlet` from SQLAlchemy | An ORM attribute was touched after a rollback or outside the async context. Capture values before `rollback()`; do not enable `pool_pre_ping` on the asyncpg engine. |
| Celery worker exits immediately on Windows | Use `--pool=threads`. |
| `uv sync` cannot download Python | Corporate TLS interception. Use an installed interpreter (`uv python list`) and keep `.python-version` at one that exists locally. |
| Integration tests all skip | The live services are not reachable at `PERSIST_URL`/`PUBSUB_URL`. |
| Integration tests all 401 | A stray `API_KEY` in the environment or `.env` differs from the running services. |
| A `pubsub.publish_event` task shows as active for minutes (`celery inspect active`) | A prefork child blocked in `getaddrinfo` while opening a DB connection (seen on Docker Desktop under a 16-process startup burst). The 120 s task time limit kills the child and requeues the message. |
| No traces or logs in Grafana | `OTEL_EXPORTER_OTLP_ENDPOINT` unset, or the container is not on `observability_default`. The service logs `telemetry: exporting to …` at startup when export is on. |

## 15. Performance

Measured with `scripts/bench_pubsub.py` on Docker Desktop (WSL2, 20 CPUs) against the compose stack: one subscriber with 20 filter subscriptions on the event type (one matches), a receiver inside the bench container, 6000 events published at concurrency 64. Numbers are the worker's **drain rate** (first to last webhook arrival with the backlog queued before the worker starts), which isolates the worker from the intake API. Runs vary by ±15% on this host; ranges are from several runs.

| Worker container | Drain rate | Worker CPU during drain |
|---|---|---|
| before (`54cb3e3`), `-P prefork -c 8` | 99–146 /s | ~700% (all 8 children saturated) |
| after, `-P threads -c 32` | 131–158 /s | ~117% (one core: GIL-bound) |
| after, `-P prefork -c 8` | ~300–410 /s | ~390% |
| after, `-P prefork -c 16` (compose default) | 400–500 /s | ~450–630% |

End to end with the compose default, same bench (N=4000, C=64): before, intake 205 /s and delivery 90 /s with p50 latency 6.8 s (queue grows); after, delivery keeps pace with intake (~330 /s, bounded by the pubsub API's two uvicorn workers and the bench publisher). A single event stream (C=1): p50 485 ms before, 11 ms after.

Independent check after the change landed, same bench run from a separate session with the **observability overlay enabled** (OTLP export of traces, metrics and logs adds per-task work) and no worker stop/start: N=6000 at C=64 gave 376 /s accepted and **212 /s delivered end to end** (before, same setup: ~90 /s); N=200 at C=1 gave **p50 12 ms** end to end (before: 109 ms). The sustained gain in that configuration is therefore about 2.3x rather than the 3-4x of the backlog-drain figures above, with the rest of the difference attributable to the telemetry overhead and to measuring intake-bound rather than backlog-bound throughput.

What changed and why:

- **Fewer round trips per event.** Before: two Celery hops (publish, then deliver), six Postgres round trips per delivery (BEGIN, two SELECTs, two UPDATEs, COMMIT), a pre-ping per checkout, a new `httpx.Client` (new TCP connection) per POST and three Redis counter commands. After: one Celery hop for the common single-subscriber case, two autocommit statements for fan-out and two for delivery, one keep-alive client per process, two pipelined counter commands. Per-task CPU, not webhook latency, is what bounds a worker on a local network, so this is where the gain comes from.
- **Cached filter parsing.** `parse_filter` ran per subscription per event (~20 µs each); it now runs once per subscription.
- **No single-row hot spot.** Every delivery updated `subscribers.last_delivery_ok_at`; with 16 workers that row lock was the next ceiling (`Lock:transactionid` in `pg_stat_activity`). It is now refreshed at most once per second.
- **Pool choice.** The threads pool was tried first (`-P threads -c 32`): it was *slower* than 8 prefork children because the remaining per-task work (SQLAlchemy, kombu, httpx, logging) is Python and serialises on the GIL at ~100 tasks/s/core. Prefork scales with cores until the parent process (one process that reads the broker, feeds children over pipes and acks) and Postgres become the limit at ~450 /s per container; beyond that, run more containers. Threads remain the right pool on Windows (`--pool=threads`) and would win only when webhook RTT, not CPU, dominates (slow remote receivers); then raise `-c` on the threads pool or split queues across a prefork worker (`-Q events`) and a threads worker (`-Q deliveries`), both of which the task routing already supports.
- **Inline first attempt** (`DELIVER_INLINE`). Saves the second hop and its three Redis commands. The cost is that a slow receiver now occupies a child of the shared pool for up to `WEBHOOK_TIMEOUT_SECONDS` during matching; set `DELIVER_INLINE=false` to restore the strict separation (the `deliveries` queue then carries every attempt).

Running the probe (from the repository root, stack up):

```bash
docker run --rm --network dbpersist_default --name bench -v "$PWD/scripts/bench_pubsub.py:/bench.py" -e PUBSUB_URL=http://pubsub-api:8002/v1 -e WEBHOOK=http://bench:9102/hook dbpersist-persistence:latest python /bench.py
```

`N`, `C` and `FILTER_SUBS` are environment variables. For the drain rate, `docker compose stop pubsub-worker`, start the bench with `-e WAIT=120`, and `docker compose start pubsub-worker` once it prints `accepted`.
