"""HMAC-SHA256 webhook signatures.

Header layout sent with every delivery:
    X-DBPersist-Timestamp: <unix seconds>
    X-DBPersist-Signature: sha256=<hex hmac(secret, f"{timestamp}.{body}")>
    X-DBPersist-Event-Id:  <event uuid>
"""

import hashlib
import hmac
import secrets
import time

SIGNATURE_HEADER = "X-DBPersist-Signature"
TIMESTAMP_HEADER = "X-DBPersist-Timestamp"
EVENT_ID_HEADER = "X-DBPersist-Event-Id"
MAX_SKEW_SECONDS = 300


def new_secret() -> str:
    return secrets.token_urlsafe(32)


def sign(secret: str, timestamp: str, body: bytes) -> str:
    mac = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256)
    return "sha256=" + mac.hexdigest()


def verify(
    secret: str, timestamp: str, body: bytes, signature: str, now: float | None = None
) -> bool:
    try:
        ts = int(timestamp)
    except ValueError:
        return False
    if abs((now if now is not None else time.time()) - ts) > MAX_SKEW_SECONDS:
        return False
    return hmac.compare_digest(sign(secret, timestamp, body), signature)
