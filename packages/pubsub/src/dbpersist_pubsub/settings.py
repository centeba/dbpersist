from dbpersist_common.settings import CommonSettings


class Settings(CommonSettings):
    pubsub_database_url: str = "postgresql+asyncpg://dbpersist:dbpersist@localhost:5442/pubsub"
    pubsub_sync_database_url: str = "postgresql+psycopg://dbpersist:dbpersist@localhost:5442/pubsub"
    idle_subscriber_minutes: int = 60
    delivery_retention_days: int = 7
    webhook_timeout_seconds: float = 10.0
    webhook_max_retries: int = 8
    webhook_backoff_max_seconds: int = 600
