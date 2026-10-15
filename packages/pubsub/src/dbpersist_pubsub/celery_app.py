from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_init, worker_process_init

from dbpersist_common.events import DELIVERIES_QUEUE, EVENTS_QUEUE, PUBLISH_TASK_NAME
from dbpersist_common.telemetry import init_telemetry, instrument_celery, instrument_sqlalchemy
from dbpersist_pubsub.settings import Settings

settings = Settings()

celery_app = Celery("dbpersist_pubsub", broker=settings.redis_url)
celery_app.conf.update(
    task_ignore_result=True,
    task_acks_late=True,
    # A task that runs for minutes is stuck (observed: a prefork child blocked forever in
    # getaddrinfo while opening a DB connection). Kill it and requeue the message rather than
    # holding it unacked for the broker's visibility timeout.
    task_time_limit=120,
    task_acks_on_failure_or_timeout=False,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=4,
    task_serializer="json",
    accept_content=["json"],
    broker_connection_retry_on_startup=True,
    task_default_queue=EVENTS_QUEUE,
    task_routes={
        PUBLISH_TASK_NAME: {"queue": EVENTS_QUEUE},
        "pubsub.deliver_webhook": {"queue": DELIVERIES_QUEUE},
        "pubsub.cleanup_idle_subscribers": {"queue": EVENTS_QUEUE},
        "pubsub.prune_deliveries": {"queue": EVENTS_QUEUE},
    },
    beat_schedule={
        "cleanup-idle-subscribers": {"task": "pubsub.cleanup_idle_subscribers", "schedule": 600.0},
        "prune-deliveries": {
            "task": "pubsub.prune_deliveries",
            "schedule": crontab(minute=0, hour=3),
        },
    },
    imports=(
        "dbpersist_pubsub.tasks.match",
        "dbpersist_pubsub.tasks.deliver",
        "dbpersist_pubsub.tasks.cleanup",
    ),
)


def _setup_worker_telemetry(**_: object) -> None:
    """Runs in every worker process (prefork children included): logging, OTLP, instrumentation."""
    from dbpersist_pubsub.db import sync_engine

    init_telemetry("dbpersist-pubsub-worker", settings)
    instrument_celery()
    instrument_sqlalchemy(sync_engine())


worker_init.connect(_setup_worker_telemetry, weak=False)
worker_process_init.connect(_setup_worker_telemetry, weak=False)
