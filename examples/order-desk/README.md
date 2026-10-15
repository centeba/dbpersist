# Order desk: an example application on dbpersist

A small trading-desk back office that shows every part of the SDK in a realistic shape:

| Piece | File | What it demonstrates |
|---|---|---|
| Domain | `orderdesk/domain.py` | registering object types with key fields, JSON schemas, and an `x-ref` reference from `Order` to `Customer` |
| Writer CLI | `orderdesk/cli.py` | inserts, a multi-object **transaction**, merge updates with an **optimistic-locking retry loop**, criteria **queries** with `and`/`like`/`in`, cursor **streaming**, reads with `expand` |
| Dashboard service | `orderdesk/dashboard.py` | the **webhook receiver** mounted in FastAPI, `active_select` (current rows plus future changes), a second filter subscription, a **key subscription** for VIP customers, version-based reconciliation in callbacks, heartbeats |
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

Open <http://localhost:9200/> to see the open-orders view, recent notifications and status changes update as you go, and <http://localhost:9200/orders/O00003> for a read-through with the customer expanded.

## How the pieces connect

```
cli.py ──writes──▶ persistence API ──event──▶ pub/sub worker ──signed webhook──▶ dashboard.py
                                                                                 (receiver verifies
                                                                                  HMAC, calls Desk)
```

- The dashboard registers itself as a **subscriber** at startup and advertises `http://host.docker.internal:9200/dbpersist/webhook`, which is how a worker inside Docker reaches a process on the host. Set `DASHBOARD_PUBLIC_HOST=127.0.0.1` when the worker runs on the host too.
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
