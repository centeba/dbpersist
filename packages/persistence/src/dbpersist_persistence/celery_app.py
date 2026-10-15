"""Producer-only Celery app: persistence enqueues events, registers no tasks."""

from celery import Celery

from dbpersist_common.events import EVENTS_QUEUE, PUBLISH_TASK_NAME


def make_producer(broker_url: str) -> Celery:
    app = Celery("dbpersist_persistence", broker=broker_url)
    app.conf.update(
        task_ignore_result=True,
        task_routes={PUBLISH_TASK_NAME: {"queue": EVENTS_QUEUE}},
        broker_connection_retry_on_startup=True,
        task_serializer="json",
        accept_content=["json"],
    )
    return app
