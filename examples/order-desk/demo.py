"""Non-interactive walkthrough: runs the dashboard in-process, drives the CLI, prints what arrived.

    uv run python demo.py

Needs the dbpersist stack running (see ../../README.md). Exit code 0 means every expected
notification was observed.
"""

import asyncio
import sys
import time

import httpx
import uvicorn

from orderdesk import cli, config
from orderdesk.dashboard import app, desk
from orderdesk.domain import ORDER

from dbpersist_sdk import DBPersistClient


async def wait_until(pred, what: str, timeout: float = 20) -> None:
    t0 = time.monotonic()
    while not pred():
        if time.monotonic() - t0 > timeout:
            raise AssertionError(f"timed out waiting for {what}; log={desk.log[-5:]}")
        await asyncio.sleep(0.1)


async def main() -> int:
    # Use a unique subscriber name so the demo can run repeatedly.
    config.SUBSCRIBER_NAME = f"order-desk-demo-{int(time.time())}"
    from orderdesk import dashboard

    dashboard.client.subscriber_name = config.SUBSCRIBER_NAME

    server = uvicorn.Server(
        uvicorn.Config(app, host=config.DASHBOARD_BIND, port=config.DASHBOARD_PORT, log_level="warning")
    )
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)
    try:
        async with DBPersistClient(**config.client_kwargs()) as c:
            before = await c.count(ORDER)
            print(f"[1] seeding 5 orders (there were {before})")
            await cli.seed(c, 5)
            await wait_until(lambda: len([e for e in desk.log if e["kind"] == "insert"]) >= 5, "5 insert webhooks")
            new_keys = sorted(k for k in desk.open_orders if int(k[1:]) > before)
            print(f"    dashboard now shows {len(desk.open_orders)} open orders; new: {new_keys}")

            first = new_keys[0]
            rec = await c.get_by_key(ORDER, first)
            half = rec.data["qty"] // 2
            print(f"[2] partial fill {first} by {half}")
            await cli.fill(c, first, half)
            await wait_until(
                lambda: desk.open_orders.get(first) is not None and desk.open_orders[first].data["status"] == "partial",
                f"{first} to show as partial",
            )
            print(f"    dashboard: {first} is {desk.open_orders[first].data['status']}, changed={desk.log[-1].get('changed')}")

            print(f"[3] complete the fill of {first}")
            await cli.fill(c, first, rec.data["qty"] - half)
            await wait_until(lambda: first not in desk.open_orders, f"{first} to leave the open set")
            print(f"    dashboard: {first} removed from open orders (status change delivered via the second subscription)")

            second = new_keys[1]
            print(f"[4] cancel {second}")
            await cli.cancel(c, second)
            await wait_until(lambda: second not in desk.open_orders, f"{second} to leave the open set")
            print(f"    dashboard: {second} removed")

            print("[5] a VIP customer record changes (key subscription)")
            vip = await c.get_by_key("Customer", "C100")
            await c.update("Customer", vip.id, vip.version, {"email": f"desk+{int(time.time())}@acme.example"}, mode="merge")
            await wait_until(lambda: any(e["type"] == "Customer" and e["key"] == "C100" for e in desk.log), "VIP update")
            print("    dashboard: VIP change received")

            print("[6] read-through with expanded customer")
            async with httpx.AsyncClient() as h:
                detail = (await h.get(f"http://127.0.0.1:{config.DASHBOARD_PORT}/orders/{new_keys[2]}")).json()
            print(f"    {new_keys[2]} belongs to {detail['_expanded']['customer']['data']['name']}")

            print("[7] queries")
            await cli.list_orders(c, status="open", symbol=None, customer=None)
            assert not desk.errors, desk.errors
        print("\nDEMO OK")
        return 0
    finally:
        server.should_exit = True
        await task


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
