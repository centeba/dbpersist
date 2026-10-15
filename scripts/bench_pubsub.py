"""Pub/sub load probe: measure intake, end-to-end delivery throughput and latency.

Runs a webhook receiver in a child process, registers one subscriber with FILTER_SUBS filter
subscriptions on a fresh type (exactly one of them matches), publishes N events to
POST /v1/publish with concurrency C, then waits for every event to arrive at the receiver.

Designed to run inside the Docker network so the worker can reach the receiver by container
name, e.g. from the repository root:

    docker run --rm --network dbpersist_default --name bench \\
      -v "$PWD/scripts/bench_pubsub.py:/bench.py" \\
      -e PUBSUB_URL=http://pubsub-api:8002/v1 -e WEBHOOK=http://bench:9102/hook \\
      dbpersist-persistence:latest python /bench.py

It also runs on the host against the published ports:

    WEBHOOK=http://host.docker.internal:9102/hook uv run python scripts/bench_pubsub.py

Env: PUBSUB_URL (default http://localhost:8102/v1), WEBHOOK (default
http://host.docker.internal:9102/hook), API_KEY (dev-api-key), N (3000), C (32),
FILTER_SUBS (20), WAIT (60 s to wait for the last delivery), RECEIVER_PORT (9102).
Exit code 0 when every event was delivered, 1 otherwise.

To measure the worker's own ceiling rather than the intake rate, stop the worker, start the
bench with a long WAIT, and start the worker once "accepted" is printed: the "drain rate" line
is then the sustained delivery throughput of that worker container.
"""

import asyncio
import json
import multiprocessing as mp
import os
import statistics
import sys
import time
import uuid
from multiprocessing.connection import Connection
from urllib.parse import urlparse

import httpx
import uvicorn

PUBSUB_URL = os.environ.get("PUBSUB_URL", "http://localhost:8102/v1").rstrip("/")
WEBHOOK = os.environ.get("WEBHOOK", "http://host.docker.internal:9102/hook")
API_KEY = os.environ.get("API_KEY", "dev-api-key")
N = int(os.environ.get("N", "3000"))
C = int(os.environ.get("C", "32"))
FILTER_SUBS = int(os.environ.get("FILTER_SUBS", "20"))
WAIT = float(os.environ.get("WAIT", "60"))
RECEIVER_PORT = int(os.environ.get("RECEIVER_PORT") or (urlparse(WEBHOOK).port or 9102))
HEADERS = {"X-API-Key": API_KEY}


def receiver_main(pipe: Connection) -> None:
    """Child process: minimal ASGI app that reports (event_id, arrival time) over the pipe."""

    async def app(scope, receive, send) -> None:  # noqa: ANN001 - raw ASGI
        if scope["type"] != "http":
            return
        if scope["method"] != "POST":  # readiness probe from the parent
            await send({"type": "http.response.start", "status": 404, "headers": []})
            await send({"type": "http.response.body", "body": b""})
            return
        body = b""
        while True:
            msg = await receive()
            body += msg.get("body", b"")
            if not msg.get("more_body"):
                break
        now = time.time()
        event_id = str(json.loads(body)["event"]["event_id"])
        pipe.send((event_id, now))
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    uvicorn.run(app, host="0.0.0.0", port=RECEIVER_PORT, log_level="warning", access_log=False)


def pct(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = min(len(s) - 1, max(0, round(p / 100 * (len(s) - 1))))
    return s[k]


def drain(pipe: Connection, arrivals: dict[str, float]) -> int:
    """Move arrivals from the receiver into `arrivals`; returns the number of posts seen."""
    posts = 0
    while pipe.poll():
        event_id, when = pipe.recv()
        arrivals.setdefault(event_id, when)
        posts += 1
    return posts


async def main() -> int:
    run = uuid.uuid4().hex[:8]
    type_name = f"Bench_{run}"
    parent_pipe, child_pipe = mp.Pipe(duplex=False)
    receiver = mp.Process(target=receiver_main, args=(child_pipe,), daemon=True)
    receiver.start()
    arrivals: dict[str, float] = {}

    async with httpx.AsyncClient(base_url=PUBSUB_URL, headers=HEADERS, timeout=30) as api:
        for _ in range(100):  # wait for the receiver to accept connections
            try:
                httpx.get(f"http://127.0.0.1:{RECEIVER_PORT}/", timeout=0.2)
                break
            except httpx.HTTPError:
                await asyncio.sleep(0.05)
        r = await api.post("/subscribers", json={"name": f"bench-{run}", "webhook_url": WEBHOOK})
        r.raise_for_status()
        subscriber_id = r.json()["id"]
        try:
            # FILTER_SUBS - 1 filters that never match, then one that always matches.
            for i in range(FILTER_SUBS - 1):
                r = await api.post(
                    f"/subscribers/{subscriber_id}/subscriptions",
                    json={
                        "type": type_name,
                        "filter": {
                            "and": [
                                {"field": "qty", "op": "gt", "value": 1_000_000 + i},
                                {"field": "book", "op": "like", "value": f"NOPE{i}%"},
                            ]
                        },
                    },
                )
                r.raise_for_status()
            r = await api.post(
                f"/subscribers/{subscriber_id}/subscriptions",
                json={"type": type_name, "filter": {"field": "qty", "op": "ge", "value": 0}},
            )
            r.raise_for_status()

            # Pre-serialise every event so the publisher loop is just HTTP.
            bodies: list[tuple[str, bytes]] = []
            for i in range(N):
                event_id = str(uuid.uuid4())
                body = {
                    "event_id": event_id,
                    "type_name": type_name,
                    "object_id": str(uuid.uuid4()),
                    "key": f"K{i}",
                    "op": "insert",
                    "version": 1,
                    "data": {"qty": i, "book": "ALPHA", "trade_id": f"K{i}"},
                }
                bodies.append((event_id, json.dumps(body, separators=(",", ":")).encode()))
            sent: dict[str, float] = {}
            sem = asyncio.Semaphore(C)
            post_headers = {"Content-Type": "application/json"}

            async def publish(event_id: str, body: bytes) -> None:
                async with sem:
                    sent[event_id] = time.time()
                    resp = await api.post("/publish", content=body, headers=post_headers)
                    resp.raise_for_status()

            print(
                f"bench {run}: N={N} C={C} filter_subs={FILTER_SUBS} "
                f"pubsub={PUBSUB_URL} webhook={WEBHOOK}",
                flush=True,
            )
            t0 = time.time()
            await asyncio.gather(*(publish(eid, body) for eid, body in bodies))
            t_accepted = time.time()

            deadline = t_accepted + WAIT
            posts = drain(parent_pipe, arrivals)
            while len(arrivals) < N and time.time() < deadline:
                await asyncio.sleep(0.05)
                posts += drain(parent_pipe, arrivals)

            latencies = [arrivals[eid] - sent[eid] for eid in sent if eid in arrivals]
            delivered = len(latencies)
            last_arrival = max(arrivals.values()) if arrivals else time.time()
            first_arrival = min(arrivals.values()) if arrivals else last_arrival
            accept_window = t_accepted - t0
            deliver_window = last_arrival - t0
            drain_window = max(last_arrival - first_arrival, 1e-9)
            hms = [time.strftime("%H:%M:%S", time.gmtime(t)) for t in (t0, t_accepted)]
            hms += [
                time.strftime("%H:%M:%S", time.gmtime(t)) for t in (first_arrival, last_arrival)
            ]
            print(
                f"timeline (UTC): start {hms[0]}  accepted {hms[1]}  arrivals {hms[2]} .. {hms[3]}"
            )
            print(f"accepted      : {N} in {accept_window:.2f}s = {N / accept_window:.1f}/s")
            print(
                f"delivered     : {delivered}/{N} in {deliver_window:.2f}s "
                f"= {delivered / deliver_window:.1f}/s end-to-end"
                + ("" if delivered == N else f"  (TIMED OUT after {WAIT:.0f}s)")
            )
            # First-to-last arrival: with a pre-queued backlog (worker started after the accept
            # phase) this is the worker's own delivery ceiling, independent of intake.
            print(f"drain rate    : {delivered / drain_window:.1f}/s (first to last arrival)")
            if latencies:
                print(
                    f"latency       : p50 {pct(latencies, 50) * 1000:.0f} ms  "
                    f"p95 {pct(latencies, 95) * 1000:.0f} ms  "
                    f"max {max(latencies) * 1000:.0f} ms  "
                    f"mean {statistics.fmean(latencies) * 1000:.0f} ms"
                )
            print(f"webhook posts : {posts} (duplicates: {posts - delivered})")
            return 0 if delivered == N else 1
        finally:
            await api.delete(f"/subscribers/{subscriber_id}")
            receiver.terminate()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
