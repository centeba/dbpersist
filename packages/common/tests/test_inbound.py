import base64
import hashlib
import hmac
import time

import pytest

from dbpersist_common.extract import MISSING, ExtractError, evaluate, get_path, render
from dbpersist_common.inbound import InboundAuth, InboundMapping, map_payload, verify_request
from dbpersist_common.signing import SIGNATURE_HEADER, TIMESTAMP_HEADER, sign

DOC = {"id": "e1", "data": {"account": {"id": "A1"}, "items": [{"sym": "X"}, {"sym": "Y"}]}}


def test_get_path_and_render() -> None:
    assert get_path(DOC, "data.account.id") == "A1"
    assert get_path(DOC, "$.data.items[1].sym") == "Y"
    assert get_path(DOC, "data.items[5]") is MISSING
    assert get_path(DOC, "nope.x") is MISSING
    assert get_path(DOC, "$") is DOC
    assert render("ACC-{data.account.id}/{id}", DOC) == "ACC-A1/e1"
    with pytest.raises(ExtractError):
        render("{missing}", DOC)
    assert evaluate("const:Account", DOC) == "Account"
    assert evaluate("id", DOC) == "e1"


ENVELOPE_MAPPING = InboundMapping(
    events_path="events",
    type="type",
    type_map={"account.updated": "Account", "account.closed": "Account"},
    key={"account_id": "payload.accountId"},
    op="type",
    op_map={"account.closed": "delete"},
    default_op="update",
    data="payload",
    external_id="id",
    occurred_at="time",
    envelope_field="_source",
)

BODY = {
    "events": [
        {
            "id": "evt-1",
            "type": "account.updated",
            "time": "2026-01-02T03:04:05Z",
            "payload": {"accountId": "ACC-9", "status": "open"},
        },
        {"id": "evt-2", "type": "account.closed", "payload": {"accountId": "ACC-9"}},
        {"id": "evt-3", "type": "order.filled", "payload": {"orderId": "O1"}},
        {"id": "evt-4", "type": "account.updated", "payload": {"status": "no key"}},
        "not an object",
    ]
}


def test_map_payload_batch() -> None:
    ok, bad = map_payload(ENVELOPE_MAPPING, BODY)
    assert [e.index for e in ok] == [0, 1]
    first, second = ok
    assert first.type_name == "Account" and first.op == "update" and first.external_id == "evt-1"
    assert first.data["account_id"] == "ACC-9" and first.data["status"] == "open"
    assert first.data["_source"] == {
        "id": "evt-1",
        "type": "account.updated",
        "time": "2026-01-02T03:04:05Z",
    }
    assert first.occurred_at is not None and first.occurred_at.year == 2026
    assert second.op == "delete"
    assert [(p.index, p.reason.split(" ")[0]) for p in bad] == [
        (2, "external"),
        (3, "key"),
        (4, "event"),
    ]


def test_map_payload_single_event_and_defaults() -> None:
    m = InboundMapping(type="const:Trade", default_type="Trade", key={"trade_id": "tid"})
    ok, bad = map_payload(m, {"tid": "T1", "qty": 5})
    assert bad == [] and ok[0].data == {"tid": "T1", "qty": 5, "trade_id": "T1"}
    assert ok[0].op == "update" and ok[0].external_id is None
    ok, bad = map_payload(InboundMapping(events_path="missing", type="t"), {"x": 1})
    assert ok == [] and "events_path" in bad[0].reason


def test_mapping_validation() -> None:
    with pytest.raises(ValueError):
        InboundMapping(type="t", key={})
    with pytest.raises(ValueError):
        InboundAuth(mode="header_secret")
    with pytest.raises(ValueError):
        InboundAuth(mode="basic", secret="s")


BODY_BYTES = b'{"a":1}'


def test_verify_header_bearer_basic() -> None:
    a = InboundAuth(mode="header_secret", secret="s3", header="X-Hook")
    assert verify_request(a, {"x-hook": "s3"}, BODY_BYTES) is None
    assert verify_request(a, {"X-Hook": "nope"}, BODY_BYTES) == "bad secret"
    assert verify_request(a, {}, BODY_BYTES) == "bad secret"
    b = InboundAuth(mode="bearer", secret="tok")
    assert verify_request(b, {"Authorization": "Bearer tok"}, BODY_BYTES) is None
    assert verify_request(b, {"Authorization": "Bearer x"}, BODY_BYTES) is not None
    c = InboundAuth(mode="basic", secret="pw", username="u")
    cred = base64.b64encode(b"u:pw").decode()
    assert verify_request(c, {"Authorization": f"Basic {cred}"}, BODY_BYTES) is None
    assert verify_request(c, {"Authorization": "Basic zzz"}, BODY_BYTES) is not None
    assert verify_request(InboundAuth(mode="none"), {}, BODY_BYTES) is None


def test_verify_hmac_variants() -> None:
    secret = "k"
    plain = InboundAuth(
        mode="hmac_sha256", secret=secret, header="X-Sig", signature_prefix="sha256="
    )
    good = "sha256=" + hmac.new(b"k", BODY_BYTES, hashlib.sha256).hexdigest()
    assert verify_request(plain, {"X-Sig": good}, BODY_BYTES) is None
    assert verify_request(plain, {"X-Sig": "sha256=00"}, BODY_BYTES) == "bad signature"
    assert verify_request(plain, {}, BODY_BYTES) == "missing X-Sig"

    ts = str(int(time.time()))
    stamped = InboundAuth(
        mode="hmac_sha256",
        secret=secret,
        header="X-Sig",
        timestamp_header="X-Ts",
        signed_payload="{timestamp}.{body}",
        encoding="base64",
    )
    mac = base64.b64encode(hmac.new(b"k", f"{ts}.".encode() + BODY_BYTES, hashlib.sha256).digest())
    assert verify_request(stamped, {"X-Sig": mac.decode(), "X-Ts": ts}, BODY_BYTES) is None
    old = str(int(time.time()) - 10_000)
    assert (
        verify_request(stamped, {"X-Sig": mac.decode(), "X-Ts": old}, BODY_BYTES)
        == "timestamp outside tolerance"
    )


def test_verify_dbpersist_chaining() -> None:
    a = InboundAuth(mode="dbpersist", secret="shared")
    ts = str(int(time.time()))
    headers = {TIMESTAMP_HEADER: ts, SIGNATURE_HEADER: sign("shared", ts, BODY_BYTES)}
    assert verify_request(a, headers, BODY_BYTES) is None
    assert verify_request(a, {**headers, SIGNATURE_HEADER: "sha256=bad"}, BODY_BYTES) is not None
