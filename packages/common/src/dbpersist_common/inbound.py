"""Inbound sources: accept event notifications from outside systems.

An inbound source describes, for one external sender, how to authenticate its webhook
requests and how to turn its envelope into dbpersist events: which part of the body is the
event (or list of events), where the external type, key, operation, payload, id and
timestamp live, and how external names map onto local object types and operations.

The models here are shared by the persistence service (which stores and executes them) and
the SDK (which creates them). ``map_payload`` is pure: it turns a parsed JSON body into a
list of ``MappedEvent`` plus a list of per-event problems, without touching any store.
"""

import base64
import hashlib
import hmac
import time
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator

from dbpersist_common.events import EventOp
from dbpersist_common.extract import MISSING, ExtractError, evaluate
from dbpersist_common.signing import SIGNATURE_HEADER, TIMESTAMP_HEADER, verify

AuthMode = Literal["none", "header_secret", "hmac_sha256", "basic", "bearer", "dbpersist"]


class InboundAuth(BaseModel):
    """How to authenticate requests from the external sender.

    - ``none``: accept anything (development only).
    - ``header_secret``: ``header`` must equal ``secret``.
    - ``bearer``: ``Authorization: Bearer <secret>``.
    - ``basic``: HTTP basic auth with ``username`` / ``secret``.
    - ``hmac_sha256``: ``header`` carries an HMAC-SHA256 of ``signed_payload`` (a template over
      ``{body}`` and ``{timestamp}``) keyed with ``secret``; ``timestamp_header`` names the
      timestamp header when the scheme uses one, checked against ``tolerance_seconds``.
    - ``dbpersist``: the signature scheme dbpersist itself emits, so one dbpersist's webhooks
      can feed another's inbound source.
    """

    model_config = ConfigDict(extra="forbid")

    mode: AuthMode = "header_secret"
    secret: str | None = None
    header: str = "X-Webhook-Secret"
    username: str | None = None
    timestamp_header: str | None = None
    signed_payload: str = "{body}"
    signature_prefix: str = ""
    encoding: Literal["hex", "base64"] = "hex"
    tolerance_seconds: int = 300

    @model_validator(mode="after")
    def _check(self) -> InboundAuth:
        if self.mode != "none" and not self.secret:
            raise ValueError(f"auth mode {self.mode!r} requires a secret")
        if self.mode == "basic" and not self.username:
            raise ValueError("auth mode 'basic' requires a username")
        return self


class InboundMapping(BaseModel):
    """How an external envelope becomes dbpersist events. Values are extract expressions."""

    model_config = ConfigDict(extra="forbid")

    events_path: str | None = Field(
        default=None, description="path to a list of events; omit when the body is one event"
    )
    type: str = Field(description="expression giving the external event type")
    type_map: dict[str, str] = Field(
        default_factory=dict, description="external type -> local object type"
    )
    default_type: str | None = Field(
        default=None, description="local type when the external type is not in type_map"
    )
    key: dict[str, str] | None = Field(
        default=None,
        description="local key field -> expression; copied into data. Omit to derive the key "
        "from data with the type's key_fields.",
    )
    op: str | None = Field(default=None, description="expression giving the external operation")
    op_map: dict[str, EventOp] = Field(
        default_factory=dict, description="external operation -> insert | update | delete"
    )
    default_op: EventOp = "update"
    data: str | None = Field(
        default=None, description="path to the payload object; omit to use the whole event"
    )
    external_id: str | None = Field(
        default=None, description="expression giving the sender's event id, used for dedupe"
    )
    occurred_at: str | None = None
    changed_fields: str | None = Field(
        default=None, description="expression giving a list of changed attribute names"
    )
    envelope_field: str | None = Field(
        default=None,
        description="when set, the event minus its payload is stored under data[envelope_field]",
    )

    @model_validator(mode="after")
    def _check(self) -> InboundMapping:
        if self.key is not None and not self.key:
            raise ValueError("key mapping must not be empty")
        return self


class MappedEvent(BaseModel):
    index: int
    external_type: str
    type_name: str
    op: EventOp
    data: dict[str, Any]
    key_values: dict[str, Any] | None = None
    external_id: str | None = None
    occurred_at: datetime | None = None
    changed_fields: list[str] = Field(default_factory=list)


class MappingProblem(BaseModel):
    index: int
    reason: str


def _one(event: Any, index: int, m: InboundMapping) -> MappedEvent:
    if not isinstance(event, dict):
        raise ExtractError("event is not an object")

    ext_type = evaluate(m.type, event)
    if ext_type is MISSING or ext_type is None:
        raise ExtractError(f"type expression {m.type!r} not found")
    ext_type = str(ext_type)
    type_name = m.type_map.get(ext_type, m.default_type)
    if type_name is None:
        raise ExtractError(f"external type {ext_type!r} is not mapped and no default_type is set")

    op: EventOp = m.default_op
    if m.op is not None:
        raw_op = evaluate(m.op, event)
        if raw_op is not MISSING and raw_op is not None:
            raw = str(raw_op)
            mapped = m.op_map.get(raw)
            if mapped is None and raw in ("insert", "update", "delete"):
                mapped = cast(EventOp, raw)
            op = mapped or m.default_op

    payload: Any = event if m.data is None else evaluate(m.data, event)
    if payload is MISSING or payload is None:
        raise ExtractError(f"data path {m.data!r} not found")
    if not isinstance(payload, dict):
        raise ExtractError("data is not an object")
    data: dict[str, Any] = dict(payload)

    key_values: dict[str, Any] | None = None
    if m.key is not None:
        key_values = {}
        for field, expr in m.key.items():
            v = evaluate(expr, event)
            if v is MISSING or v is None:
                raise ExtractError(f"key field {field!r}: expression {expr!r} not found")
            key_values[field] = v
        data.update(key_values)

    if m.envelope_field:
        envelope = {k: v for k, v in event.items() if m.data is None or v is not payload}
        data[m.envelope_field] = envelope

    external_id = None
    if m.external_id is not None:
        v = evaluate(m.external_id, event)
        external_id = None if v is MISSING or v is None else str(v)

    occurred_at = None
    if m.occurred_at is not None:
        v = evaluate(m.occurred_at, event)
        if v is not MISSING and v is not None:
            try:
                occurred_at = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            except ValueError as exc:
                raise ExtractError(f"occurred_at {v!r} is not ISO-8601") from exc

    changed: list[str] = []
    if m.changed_fields is not None:
        v = evaluate(m.changed_fields, event)
        if isinstance(v, list):
            changed = [str(x) for x in v]

    return MappedEvent(
        index=index,
        external_type=ext_type,
        type_name=type_name,
        op=op,
        data=data,
        key_values=key_values,
        external_id=external_id,
        occurred_at=occurred_at,
        changed_fields=changed,
    )


def map_payload(m: InboundMapping, body: Any) -> tuple[list[MappedEvent], list[MappingProblem]]:
    """Apply a mapping to a parsed body; problems are per event and never block the rest."""
    if m.events_path is None:
        events: Any = [body]
    else:
        events = evaluate(m.events_path, body)
        if events is MISSING or events is None:
            return [], [MappingProblem(index=0, reason=f"events_path {m.events_path!r} not found")]
        if isinstance(events, dict):
            events = [events]
        if not isinstance(events, list):
            return [], [MappingProblem(index=0, reason="events_path does not point at a list")]

    ok: list[MappedEvent] = []
    bad: list[MappingProblem] = []
    for i, ev in enumerate(events):
        try:
            ok.append(_one(ev, i, m))
        except ExtractError as exc:
            bad.append(MappingProblem(index=i, reason=str(exc)))
    return ok, bad


def _hget(headers: Mapping[str, str], name: str) -> str | None:
    for k, v in headers.items():
        if k.lower() == name.lower():
            return v
    return None


def verify_request(auth: InboundAuth, headers: Mapping[str, str], body: bytes) -> str | None:
    """Return None when the request is authentic, otherwise a short reason."""
    secret = auth.secret or ""
    match auth.mode:
        case "none":
            return None
        case "header_secret":
            got = _hget(headers, auth.header)
            return None if got is not None and hmac.compare_digest(got, secret) else "bad secret"
        case "bearer":
            got = _hget(headers, "Authorization") or ""
            token = got[7:] if got.lower().startswith("bearer ") else ""
            return None if token and hmac.compare_digest(token, secret) else "bad bearer token"
        case "basic":
            got = _hget(headers, "Authorization") or ""
            if not got.lower().startswith("basic "):
                return "missing basic credentials"
            try:
                user, _, pwd = base64.b64decode(got[6:]).decode().partition(":")
            except Exception:  # noqa: BLE001
                return "malformed basic credentials"
            ok = hmac.compare_digest(user, auth.username or "") and hmac.compare_digest(pwd, secret)
            return None if ok else "bad basic credentials"
        case "hmac_sha256":
            sig = _hget(headers, auth.header)
            if sig is None:
                return f"missing {auth.header}"
            if auth.signature_prefix and sig.startswith(auth.signature_prefix):
                sig = sig[len(auth.signature_prefix) :]
            ts = ""
            if auth.timestamp_header:
                ts = _hget(headers, auth.timestamp_header) or ""
                if not ts:
                    return f"missing {auth.timestamp_header}"
                try:
                    if abs(time.time() - float(ts)) > auth.tolerance_seconds:
                        return "timestamp outside tolerance"
                except ValueError:
                    return "bad timestamp"
            signed = auth.signed_payload.replace("{timestamp}", ts).encode()
            signed = signed.replace(b"{body}", body)
            mac = hmac.new(secret.encode(), signed, hashlib.sha256).digest()
            expected = mac.hex() if auth.encoding == "hex" else base64.b64encode(mac).decode()
            return None if hmac.compare_digest(expected, sig) else "bad signature"
        case "dbpersist":
            ts = _hget(headers, TIMESTAMP_HEADER) or ""
            sig = _hget(headers, SIGNATURE_HEADER) or ""
            return None if verify(secret, ts, body, sig) else "bad dbpersist signature"
    return "unsupported auth mode"


class InboundPull(BaseModel):
    """Optional polling of the sender's event feed, for when it stops pushing or for backfill.

    Every ``interval_seconds`` the persistence service requests ``url`` with ``headers``, passing
    the stored cursor as query parameter ``cursor_param`` (omitted on the first pull or when
    ``cursor_param`` is empty), enqueues the response body as one inbound message (mapped with
    the source's normal mapping, so ``events_path`` applies), and stores the next cursor found
    at ``cursor_path`` in the response. Dedupe on ``external_id`` makes push and pull overlap
    harmless.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    url: str
    method: Literal["GET", "POST"] = "GET"
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, str] = Field(default_factory=dict)
    body: dict[str, Any] | None = Field(default=None, description="JSON body for POST pulls")
    interval_seconds: int = Field(default=60, ge=5)
    cursor_param: str = Field(default="cursor", description="query/body parameter for the cursor")
    cursor_path: str | None = Field(
        default=None, description="expression over the response giving the next cursor"
    )
    initial_cursor: str | None = None
    timeout_seconds: float = 20.0
