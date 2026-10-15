# Order desk: an example application on dbpersist

A small trading-desk back office that shows every part of the SDK in a realistic shape:

| Piece | File | What it demonstrates |
|---|---|---|
| Domain | `orderdesk/domain.py` | registering object types with key fields, JSON schemas, and an `x-ref` reference from `Order` to `Customer` |
| Writer CLI | `orderdesk/cli.py` | inserts, a multi-object **transaction**, merge updates with an **optimistic-locking retry loop**, criteria **queries** with `and`/`like`/`in`, cursor **streaming**, reads with `expand` |
| Dashboard service | `orderdesk/dashboard.py` | the **webhook receiver** mounted in FastAPI, `active_select` (current rows plus future changes), a second filter subscription, a **key subscription** for VIP customers, version-based reconciliation in callbacks, heartbeats, a **server-sent events** stream of every notification, action endpoints, and a proxy to the pub/sub admin API |
| React UI | `ui/` | a live view of the pub/sub flow: open orders kept current by webhooks, a real-time notification feed, your subscriptions, every pub/sub client with pending and failed delivery counts, platform-wide counters, and buttons to seed, fill and cancel orders so you can watch the round trip |
| Demo | `demo.py` | runs the dashboard in-process, drives the CLI, and asserts the notifications arrive |

## Run it

Start dbpersist first (from the repository root):

```bash
docker compose up -d --build
```

Then, in this directory:

```bash
uv sync
uv run python demo.py
```

Expected output ends with `DEMO OK` after seven numbered steps: seeding five orders in one transaction, a partial fill, a complete fill, a cancellation, a VIP customer change, a read-through with the customer expanded, and a query.

For the interactive version, run the dashboard in one terminal:

```bash
uv run uvicorn orderdesk.dashboard:app --host 0.0.0.0 --port 9200
```

and drive it from another:

```bash
uv run python -m orderdesk.cli seed --orders 10
uv run python -m orderdesk.cli list --status open
uv run python -m orderdesk.cli fill O00001 100
uv run python -m orderdesk.cli cancel O00002
uv run python -m orderdesk.cli history O00003
uv run python -m orderdesk.cli stream
```

## The React UI

```bash
cd ui
npm install
npm run build        # the dashboard serves the build at http://localhost:9200/app/
```

Open <http://localhost:9200/app/>. For development with hot reload, `npm run dev` serves it on <http://localhost:5174> and proxies `/api` to the dashboard.

What you are looking at:

- **Counters** across the top come from the pub/sub admin API (events received, deliveries created, delivered, failed) plus this subscriber's own pending and failed deliveries.
- **Generate traffic** calls the dashboard's action endpoints, which write through persistence exactly as the CLI does. Nothing in the page updates from those calls directly; everything below changes only when the webhook comes back, so what you see is the pub/sub round trip.
- **Open orders** is the dashboard's in-memory view, rebuilt from `active_select` at startup and then maintained from notifications. Each row has half-fill, fill-all and cancel buttons; click a row for a read-through from persistence with the customer expanded.
- **Live notifications** is a server-sent events stream from the dashboard: one line per webhook with the operation, type, key, version and `changed_fields`.
- **My subscriptions** and **Pub/sub clients** show how this subscriber is registered and how deliveries are going for every client on the platform.

The browser never holds the dbpersist API key; the dashboard proxies the admin calls.

The JSON endpoints behind the UI are `GET /api/state`, `GET /api/events` (SSE), `GET /api/orders/{id}`, `GET /api/pubsub/{stats,clients,subscriptions}` and `POST /api/actions/{seed,fill,cancel}`.

## How the pieces connect

```
cli.py / UI actions ──writes──▶ persistence API ──event──▶ pub/sub worker ──signed webhook──▶ dashboard.py ──SSE──▶ React UI
                                                                                              (receiver verifies
                                                                                               HMAC, calls Desk)
```

- The dashboard registers itself as a **subscriber** at startup; if a previous instance left a subscriber with the same name, it replaces it (the secret is only issued at registration). It advertises `http://host.docker.internal:9200/dbpersist/webhook`, which is how a worker inside Docker reaches a process on the host. Set `DASHBOARD_PUBLIC_HOST=127.0.0.1` when the worker runs on the host too.
- `active_select(Order, status in [open, partial])` returns the current open orders and subscribes to changes matching the same filter. Because an order that becomes `filled` no longer matches that filter, the dashboard adds a second subscription for `status in [filled, cancelled]` so the view also shrinks. This is the general pattern: subscribe to both sides of any state transition you need to observe.
- The key subscription on VIP customers delivers every change to those records, whatever the change is.
- Callbacks compare `record.version` with what they hold before applying an update. Deliveries are at-least-once and not strictly ordered under retries, so this check is what keeps the view correct.
- `cli.py fill` reads the order, computes the new state, and sends the update with the version it read. If another writer got there first the service returns a version conflict and the CLI simply re-reads and retries.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `PERSIST_URL` / `PUBSUB_URL` | `http://localhost:8101` / `8102` | dbpersist endpoints |
| `API_KEY` | `dev-api-key` | shared API key |
| `DASHBOARD_PORT` | `9200` | dashboard listen port |
| `DASHBOARD_BIND` | `0.0.0.0` | bind address |
| `DASHBOARD_PUBLIC_HOST` | `host.docker.internal` | hostname the pub/sub worker uses to call back |
| `SUBSCRIBER_NAME` | `order-desk-dashboard` | subscriber name registered with pub/sub |
