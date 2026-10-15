"""The writer side: a command-line tool that creates and changes orders.

Commands
    seed                      create customers and a batch of orders in one transaction
    fill ORDER QTY            record a (partial) fill with an optimistic-locking retry loop
    cancel ORDER              cancel an order
    list [--status S] [--symbol X] [--customer C]   criteria queries
    stream                    page through every order with the keyset cursor
    history ORDER             show an order with its customer expanded

Every write goes through persistence, which publishes the change; the dashboard picks it up.
"""

import argparse
import asyncio
import json
import random
import sys

from dbpersist_sdk import DBPersistClient, DuplicateKey, NotFound, VersionConflict
from orderdesk import config
from orderdesk.domain import CUSTOMER, ORDER, ensure_types

CUSTOMERS = [
    {"customer_id": "C100", "name": "Acme Pension", "tier": "vip", "email": "ops@acme.example"},
    {"customer_id": "C200", "name": "Blue Harbour", "tier": "standard"},
    {"customer_id": "C300", "name": "Cobalt Capital", "tier": "standard"},
]
SYMBOLS = ["ACME", "BLUE", "COBL", "DELT", "ECHO"]


def _print(obj: object) -> None:
    print(json.dumps(obj, indent=2, default=str))


async def seed(client: DBPersistClient, n_orders: int) -> None:
    await ensure_types(client)
    customers: dict[str, str] = {}  # customer_id -> object id
    for c in CUSTOMERS:
        try:
            rec = await client.insert(CUSTOMER, c)
        except DuplicateKey:
            rec = await client.get_by_key(CUSTOMER, c["customer_id"])
        customers[c["customer_id"]] = str(rec.id)

    existing = await client.count(ORDER)
    # One transaction: either every order is created and published, or none is.
    async with client.transaction() as tx:
        for i in range(n_orders):
            cust = random.choice(CUSTOMERS)["customer_id"]
            tx.insert(
                ORDER,
                {
                    "order_id": f"O{existing + i + 1:05d}",
                    "customer": customers[cust],
                    "symbol": random.choice(SYMBOLS),
                    "side": random.choice(["buy", "sell"]),
                    "qty": random.choice([100, 250, 500, 1000]),
                    "filled": 0,
                    "limit_price": round(random.uniform(10, 200), 2),
                    "status": "open",
                    "notes": [],
                },
            )
    assert tx.result is not None
    print(f"seeded {len(tx.result.results)} orders in transaction {tx.result.transaction_id}")


async def fill(client: DBPersistClient, order_id: str, qty: int) -> None:
    """Apply a fill. Concurrent fills on the same order are serialised by the version check."""
    for _ in range(5):
        rec = await client.get_by_key(ORDER, order_id)
        filled = rec.data.get("filled", 0) + qty
        if filled > rec.data["qty"]:
            sys.exit(f"fill of {qty} would exceed order qty {rec.data['qty']}")
        status = "filled" if filled == rec.data["qty"] else "partial"
        try:
            new = await client.update(
                ORDER,
                rec.id,
                rec.version,
                {
                    "filled": filled,
                    "status": status,
                    "notes": [*rec.data.get("notes", []), f"fill {qty}"],
                },
                mode="merge",
            )
        except VersionConflict as exc:
            print(
                f"version conflict (someone else changed it, now v{exc.current_version}); retrying"
            )
            continue
        print(
            f"{order_id}: filled {new.data['filled']}/{new.data['qty']} "
            f"-> {new.data['status']} (v{new.version})"
        )
        return
    sys.exit("gave up after 5 conflicts")


async def cancel(client: DBPersistClient, order_id: str) -> None:
    rec = await client.get_by_key(ORDER, order_id)
    if rec.data["status"] in ("filled", "cancelled"):
        sys.exit(f"{order_id} is already {rec.data['status']}")
    new = await client.update(ORDER, rec.id, rec.version, {"status": "cancelled"}, mode="merge")
    print(f"{order_id}: cancelled (v{new.version})")


async def list_orders(
    client: DBPersistClient, status: str | None, symbol: str | None, customer: str | None
) -> None:
    clauses: list[dict] = []
    if status:
        clauses.append({"field": "status", "op": "eq", "value": status})
    if symbol:
        clauses.append({"field": "symbol", "op": "like", "value": f"{symbol.upper()}%"})
    if customer:
        cust = await client.get_by_key(CUSTOMER, customer)
        clauses.append({"field": "customer", "op": "eq", "value": str(cust.id)})
    flt = None if not clauses else clauses[0] if len(clauses) == 1 else {"and": clauses}
    rows = await client.select(ORDER, flt, order_by=[{"field": "key"}], limit=50)
    total = await client.count(ORDER, flt)
    for r in rows:
        d = r.data
        print(
            f"{r.key}  {d['side']:4} {d['qty']:>5} {d['symbol']:<5} "
            f"filled={d.get('filled', 0):<5} {d['status']:<9} v{r.version}"
        )
    print(f"{len(rows)} shown of {total}")


async def stream(client: DBPersistClient) -> None:
    n = 0
    async for r in client.select_iter(ORDER, page_size=100):
        n += 1
    print(f"streamed {n} orders through the cursor")


async def history(client: DBPersistClient, order_id: str) -> None:
    try:
        rec = await client.get_by_key(ORDER, order_id, expand=["customer"])
    except NotFound:
        sys.exit(f"no order {order_id}")
    _print({"id": str(rec.id), "version": rec.version, **rec.data})


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        prog="orderdesk", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--orders", type=int, default=8)
    s = sub.add_parser("fill")
    s.add_argument("order")
    s.add_argument("qty", type=int)
    s = sub.add_parser("cancel")
    s.add_argument("order")
    s = sub.add_parser("list")
    s.add_argument("--status")
    s.add_argument("--symbol")
    s.add_argument("--customer")
    sub.add_parser("stream")
    s = sub.add_parser("history")
    s.add_argument("order")
    a = p.parse_args(argv)

    async def run() -> None:
        async with DBPersistClient(**config.client_kwargs()) as client:
            match a.cmd:
                case "seed":
                    await seed(client, a.orders)
                case "fill":
                    await fill(client, a.order, a.qty)
                case "cancel":
                    await cancel(client, a.order)
                case "list":
                    await list_orders(client, a.status, a.symbol, a.customer)
                case "stream":
                    await stream(client)
                case "history":
                    await history(client, a.order)

    asyncio.run(run())


if __name__ == "__main__":
    main()
