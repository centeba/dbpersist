# dbpersist test cases

Three layers of tests cover the system:

| Layer | Where | Needs | Run |
|---|---|---|---|
| Unit and API tests | `packages/*/tests` | Postgres container only (`persist_test`, `pubsub_test` databases); Celery runs eagerly, webhooks are mocked | `uv run pytest packages` |
| Integration scenarios | `tests/integration` | The live stack: persistence API, pubsub API, Celery worker, Postgres, Redis | `uv run pytest tests/integration` |
| End-to-end smoke | `scripts/smoke_e2e.py` | Same as integration | `uv run python scripts/smoke_e2e.py` |

The integration directory skips itself when the services are not reachable, so the default `uv run pytest` is safe on any machine.

## Running the integration scenarios

```bash
docker compose up -d --build          # or run the services via uv as described in README.md
uv run pytest tests/integration -v
```

The harness starts a webhook receiver in-process on port `9101`, bound to all interfaces, and registers two subscribers with pubsub. Environment overrides:

| Variable | Default | Meaning |
|---|---|---|
| `PERSIST_URL` / `PUBSUB_URL` | `http://localhost:8101` / `8102` | service endpoints |
| `API_KEY` | `dev-api-key` | shared API key |
| `WEBHOOK_HOST` | `host.docker.internal` | hostname the worker uses to call the receiver; use `127.0.0.1` when the worker runs on the host |
| `WEBHOOK_PORT` | `9101` | receiver port |
| `DELIVERY_TIMEOUT` | `20` | seconds to wait for an expected webhook |
| `NEGATIVE_WAIT` | `2` | seconds to wait before declaring a webhook did not arrive |

Every test registers fresh object types and subscribers with unique names, so the suite can run repeatedly against a shared database without cleanup.

## Persistence scenarios (`tests/integration/test_persistence_scenarios.py`)

| ID | Scenario | Steps | Expected |
|---|---|---|---|
| P1 | Type registry | Register a type with two key fields; register the same name again; register an invalid name and an empty key list; fetch a known and an unknown type | 201, then 409, 422, 422; fetch returns the type; unknown type is 404 |
| P2 | Insert and read | Insert an object; read by id and by key; insert the same key again; read unknown id and key | Version 1 and derived key; both reads return the object; duplicate is 409; unknowns are 404 |
| P2b | Composite keys | Register a type keyed on two fields; insert; read by the two key values; insert the same pair; insert a different pair | Key is the two values joined by the unit separator; duplicate pair is 409; different pair succeeds |
| P3 | Schema and key validation | Insert with a missing required field, a wrongly typed field, and a null key field | Each attempt is rejected with 422 `invalid_data` |
| P4 | Replace and merge updates | Merge a partial document into a nested object; replace the whole document; try to change a key field | Merge deep-merges and bumps the version; replace drops absent fields and bumps again; key change is 422 |
| P5 | Optimistic locking | Update with the current version, then update and delete with a stale version | First update succeeds; stale update and delete return 409 with the current version; data is unchanged |
| P6 | Soft delete and key reuse | Delete an object; read it; count; insert the same key again | Read is 404; count is 0; the new object has a new id and version 1 |
| P7 | Criteria queries | Seed five rows and run 17 filters: comparisons, `in`, `like`/`not_like`, null tests, booleans, nested paths, datetimes, `and`/`or`, and the `key` and `version` column fields | Each filter returns exactly the expected keys and `count` agrees |
| P7b | Invalid filters | Query with a clause missing its value and with an illegal field name | Both rejected with 422 |
| P8 | Ordering and paging | Order by key descending with a limit; order ascending with an offset; order by a numeric field; stream all rows through the cursor in pages of two; send a bad cursor and a zero limit | Correct slices; numeric field sorts numerically, not as text; the stream yields every row exactly once; bad inputs are 422 |
| P9 | Transactions | Commit an insert, a merge update and a nested-scope insert together; commit a batch whose second op has a duplicate key; commit a batch whose second op has a stale version; raise inside a transaction block | First commit applies all three with one version bump; both failing batches leave the row count unchanged; the raised block commits nothing |
| P10 | Reference expansion | Insert an account, insert a trade referencing it, read and query with `expand=account`; expand a non-reference field | The referenced document is inlined under `_expanded`; the bad field is 422 |
| P11 | Publish-only | Publish an update without persisting; publish without a key field | An event id is returned and the count stays 0; the keyless publish is 422 |
| P12 | Authentication | Call the API without and with a wrong `X-API-Key`; call `/health` without a key | 401, 401, 200 |
| P13 | Concurrent updates | Ten coroutines each read the object and apply a merge, retrying on conflict | Final version is 11, the counter is 10, every writer's mark is present, and at least one conflict was detected |

## Pub/sub scenarios (`tests/integration/test_pubsub_scenarios.py`)

| ID | Scenario | Steps | Expected |
|---|---|---|---|
| S1 | Subscriber lifecycle | Register; register the same name; register with a bad URL; heartbeat; add a subscription; delete the subscriber; read it and delete its subscription | 201 with a secret, 409, 422; heartbeat advances `last_seen_at`; after delete the subscriber is 404 and its subscription was cascaded |
| S2 | Subscription validation | Post subscriptions with neither filter nor keys, empty keys, an invalid filter, both filter and keys, an invalid `by`; then a valid one; subscribe under an unknown subscriber | 422 for each invalid body; 201 for the valid one; unknown subscriber is 404 |
| S3 | Filter routing | Subscribe to `qty > 100`; insert a matching and a non-matching object | Exactly one `on_insert`, carrying type, version 1 and the data; nothing arrives for the non-match |
| S4 | Key and id subscriptions | Subscribe by key and, separately, by object id; update the target; insert an unrelated object | Both subscriptions receive the update; neither receives the unrelated insert |
| S5 | One delivery per subscriber | Create two subscriptions on one subscriber that both match; insert | One callback, no pending or failed deliveries for that subscriber |
| S6 | Update and delete payloads | Insert, merge-update, replace-update (dropping a field), delete | Updates arrive with `changed_fields` of `["qty"]` then `["book"]`; the delete carries the last document image and the final version |
| S7 | Transaction events | Commit three inserts in one transaction; attempt a batch that fails on a duplicate key | All three inserts are delivered; nothing from the failed batch is delivered |
| S8 | Retry until the webhook recovers | Point a second subscriber at a path that fails the next two requests with 500; insert a matching object | The event arrives on the third attempt; the subscriber shows no pending or failed deliveries and a `last_delivery_ok_at` |
| S9 | Admin views | Read criteria by client and the client list; insert a matching object; read stats; run idle cleanup; set the log level | Criteria and client counts are correct; `received`, `matched` and `delivered` all increase; the active subscriber is not removed; log level is accepted |
| S10 | Unsubscribe | Subscribe by key, insert, unsubscribe, update | The insert is delivered; the update is not |
| S11 | Publish-only reaches subscribers | Subscribe to `qty = 42`; publish an update without persisting | The update is delivered with `changed_fields` and no object id; the count stays 0 |
| S12 | Active select | Insert; call `active_select` with a matching filter; insert another match | The initial select returns the existing row and the later insert is delivered |

## Inbound scenarios (`tests/integration/test_inbound_scenarios.py`)

| ID | Scenario | Steps | Expected |
|---|---|---|---|
| I1 | External events through the queue | Register a source (shared-secret auth, batched envelope mapping, auto-registered type); subscribe to the type; POST an `account.opened` event; POST it again; POST an update and a close in one batch | Each POST is answered 202; the first produces an `on_insert` with the envelope stored under `_source`; the redelivery is processed as a duplicate and produces no webhook; the batch produces an `on_update` with `changed_fields == ["status"]` and an `on_delete` |
| I2 | Signed publish-only source | Register a source with HMAC-SHA256 over `timestamp.body`, mode publish, synchronous; POST with a bad and a good signature | 401 then 200; the subscriber receives the update with no object id and nothing is stored |
| I3 | Dry run and management | Create a source, dry-run a two-event payload, list sources, delete | One mapped event and one problem reported, the unknown local type named, source visible in the list |

## What the unit layer already covers

- `packages/common/tests`: filter DSL validation, the in-memory evaluator table, compiled SQL for every operator, and a parity test that the SQL and the evaluator agree on the same fixture data.
- `packages/common/tests/test_inbound.py`: the extract language, batch and single-event mapping, every authentication mode including HMAC variants and dbpersist chaining.
- `packages/persistence/tests/test_inbound.py`: source CRUD with secret masking, ingress authentication and unknown/disabled sources, the synchronous upsert lifecycle (auto-registered type, rejects, duplicates, merge with changed fields, unchanged skip, delete), publish mode with HMAC, dry run, queue mode ordering and 202, transient-failure retry with order preserved, quarantine after max attempts and operator retry, pull with cursor and interval, pull failure recording.
- `packages/persistence/tests`: all CRUD routes, schema validation, version conflicts, replace and merge semantics with `changed_fields`, filter parity against Postgres, cursor and offset paging, transaction atomicity, publish-only, reference expansion, API key enforcement. `test_outbox.py` covers the transactional outbox: the event row is written in the write's own transaction and is absent after a version conflict, a duplicate key and a failed multi-op transaction; the relay sends in id order (one object's events in version order), marks `sent_at` and `attempts`, and honours the batch size; a publisher failure commits the rows already sent, records `attempts` and `last_error` on the failing row, leaves the rest untouched and the next relay resumes from it without resending; publish-only goes through the outbox; pruning removes only sent rows past retention; and an app with `OUTBOX_RELAY_ENABLED` relays a committed insert from its background loop. `test_pubsub_switch.py` builds the app with `PUBSUB_ENABLED=false` and an unreachable broker URL and checks that inserts, updates, transactions and publish-only succeed and write no outbox rows.
- `packages/pubsub/tests`: subscriber and subscription APIs, matching and per-subscriber dedupe, signed webhook delivery, retry bookkeeping, terminal failure and replay, idle cleanup and delivery pruning, stats counters, admin endpoints.
- `packages/sdk/tests`: request shapes, cursor following, error mapping, nested transactions, subscribe-before-select ordering, signature verification and callback dispatch in the receiver.

## Not covered automatically

- Terminal delivery failure after all retries in a live worker (unit-tested only; the live backoff would take minutes).
- Celery beat schedules firing on time.
- Behaviour under a Postgres outage mid-request. (A Redis outage is covered at the unit level by the outbox relay-failure test: writes commit and events wait.)
