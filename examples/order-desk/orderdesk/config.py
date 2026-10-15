"""Connection settings, all overridable through the environment."""

import os

PERSIST_URL = os.environ.get("PERSIST_URL", "http://localhost:8101")
PUBSUB_URL = os.environ.get("PUBSUB_URL", "http://localhost:8102")
API_KEY = os.environ.get("API_KEY", "dev-api-key")

# Where the dashboard listens, and how the pub/sub worker should reach it. When the worker runs
# in Docker and the dashboard on the host, the worker must use host.docker.internal.
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "9200"))
DASHBOARD_BIND = os.environ.get("DASHBOARD_BIND", "0.0.0.0")
DASHBOARD_PUBLIC_HOST = os.environ.get("DASHBOARD_PUBLIC_HOST", "host.docker.internal")
WEBHOOK_URL = f"http://{DASHBOARD_PUBLIC_HOST}:{DASHBOARD_PORT}/dbpersist/webhook"

SUBSCRIBER_NAME = os.environ.get("SUBSCRIBER_NAME", "order-desk-dashboard")


def client_kwargs() -> dict[str, str]:
    return {"persist_url": PERSIST_URL, "pubsub_url": PUBSUB_URL, "api_key": API_KEY}
