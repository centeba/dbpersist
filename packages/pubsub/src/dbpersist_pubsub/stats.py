"""Traffic counters: kept in Redis for the admin API and mirrored to OpenTelemetry metrics."""

from typing import Protocol, cast

import redis

from dbpersist_common.telemetry import get_meter

COUNTERS = ("received", "matched", "delivered", "failed")

_meter = get_meter("dbpersist.pubsub")
_OTEL_COUNTERS = {
    "received": _meter.create_counter(
        "dbpersist.pubsub.events.received", unit="1", description="Events received from producers"
    ),
    "matched": _meter.create_counter(
        "dbpersist.pubsub.deliveries.created",
        unit="1",
        description="Deliveries created by matching",
    ),
    "delivered": _meter.create_counter(
        "dbpersist.pubsub.deliveries.delivered",
        unit="1",
        description="Webhook deliveries acknowledged",
    ),
    "failed": _meter.create_counter(
        "dbpersist.pubsub.deliveries.failed",
        unit="1",
        description="Deliveries that exhausted retries",
    ),
}


def _otel_incr(name: str, n: int) -> None:
    counter = _OTEL_COUNTERS.get(name)
    if counter is not None:
        counter.add(n)


class Stats(Protocol):
    def incr(self, name: str, n: int = 1) -> None: ...
    def get(self, name: str) -> int: ...


class RedisStats:
    def __init__(self, url: str, prefix: str = "dbpersist:stats:") -> None:
        self._r = redis.Redis.from_url(url)
        self._prefix = prefix

    def incr(self, name: str, n: int = 1) -> None:
        _otel_incr(name, n)
        try:
            self._r.incrby(self._prefix + name, n)
        except redis.RedisError:
            pass

    def get(self, name: str) -> int:
        try:
            v = self._r.get(self._prefix + name)
        except redis.RedisError:
            return 0
        return int(cast(bytes, v)) if v else 0


class MemoryStats:
    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    def incr(self, name: str, n: int = 1) -> None:
        _otel_incr(name, n)
        self.counts[name] = self.counts.get(name, 0) + n

    def get(self, name: str) -> int:
        return self.counts.get(name, 0)


_stats: Stats | None = None


def get_stats() -> Stats:
    global _stats
    if _stats is None:
        from dbpersist_pubsub.settings import Settings

        _stats = RedisStats(Settings().redis_url)
    return _stats


def set_stats(s: Stats) -> None:
    global _stats
    _stats = s
