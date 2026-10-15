from dbpersist_common.settings import CommonSettings


class Settings(CommonSettings):
    persist_database_url: str = "postgresql+asyncpg://dbpersist:dbpersist@localhost:5442/persist"
    db_pool_size: int = 10
    db_max_overflow: int = 10

    # Change-event publishing to the pub/sub service. With this off, persistence needs no
    # Redis and no pub/sub deployment: writes, queries, transactions and inbound sources
    # work unchanged, and committed changes simply emit no events.
    pubsub_enabled: bool = True

    # Inbound queue processor (runs inside the API process; set false on instances that
    # should only serve HTTP, true on at least one)
    inbound_processor_enabled: bool = True
    inbound_tick_seconds: float = 1.0
    inbound_max_attempts: int = 10
    inbound_backoff_max_seconds: int = 300
