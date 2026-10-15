"""Test helpers shared by the pubsub and SDK test suites."""

import httpx


class WebhookSink:
    """Fake webhook endpoint for httpx.MockTransport: records requests, can be told to fail."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.fail_times = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.fail_times > 0:
            self.fail_times -= 1
            return httpx.Response(500, text="boom")
        return httpx.Response(200, json={"ok": True})
