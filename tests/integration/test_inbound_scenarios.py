"""Inbound scenarios I1-I3 (see docs/TEST_CASES.md): external webhooks become dbpersist events."""

import asyncio
import hashlib
import hmac
import json
import time

import httpx
import pytest

from dbpersist_sdk import DBPersistClient

from .conftest import API_KEY, PERSIST_URL, Collector, unique


async def _wait_done(
    sdk: DBPersistClient, source: str, message_id: int, timeout: float = 20
) -> dict:
    """Poll the source's queue until the given message is processed; return its row."""
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient(
        base_url=f"{PERSIST_URL}/v1", headers={"X-API-Key": API_KEY}, timeout=30
    ) as admin:
        while time.monotonic() < deadline:
            rows = (await admin.get(f"/inbound-sources/{source}/messages?status=done")).json()
            for m in rows:
                if m["id"] == message_id:
                    return m
            await asyncio.sleep(0.25)
    raise AssertionError(f"message {message_id} of {source} was not processed in time")


pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


def _mapping(local_type: str) -> dict:
    return {
        "events_path": "events",
        "type": "type",
        "type_map": {
            "account.opened": local_type,
            "account.updated": local_type,
            "account.closed": local_type,
        },
        "key": {"account_id": "payload.accountId"},
        "op": "type",
        "op_map": {"account.opened": "insert", "account.closed": "delete"},
        "data": "payload",
        "external_id": "id",
        "occurred_at": "time",
        "envelope_field": "_source",
    }


def _event(id_: str, type_: str, **payload: object) -> dict:
    return {"id": id_, "type": type_, "time": "2026-03-01T10:00:00Z", "payload": payload}


# I1 ------------------------------------------------------------------------------------------
async def test_i1_external_events_flow_to_subscribers(sdk: DBPersistClient) -> None:
    local_type = unique("ExtAccount")
    source = unique("broker").lower()
    secret = "s-" + source
    await sdk.create_inbound_source(
        source,
        _mapping(local_type),
        {"mode": "header_secret", "secret": secret, "header": "X-Hook-Secret"},
        auto_register_types=True,
    )
    cb = Collector()
    try:
        # the type does not exist yet; the first inbound event auto-registers it, so subscribe
        # by filter on the (future) type name, which pub/sub accepts without the type existing
        handle = await sdk.subscribe(
            local_type, {"field": "account_id", "op": "like", "value": "A%"}, callbacks=cb
        )
        async with httpx.AsyncClient(base_url=f"{PERSIST_URL}/v1", timeout=30) as ext:
            r = await ext.post(
                f"/inbound/{source}",
                json={"events": [_event("e1", "account.opened", accountId="A1", status="open")]},
                headers={"X-Hook-Secret": secret},
            )
            assert r.status_code == 202, r.text
            assert r.json()["queued"] is True
            await cb.wait_for(
                lambda: "A1" in cb.keys("insert"), "insert webhook from inbound event"
            )
            rec = cb.of("insert")[0][1]
            assert rec.type_name == local_type and rec.data["_source"]["id"] == "e1"

            # redelivery is a duplicate and produces no second webhook
            r = await ext.post(
                f"/inbound/{source}",
                json={"events": [_event("e1", "account.opened", accountId="A1", status="open")]},
                headers={"X-Hook-Secret": secret},
            )
            dup_id = r.json()["message_id"]
            await cb.expect_none("update", "A1")
            assert cb.keys("insert").count("A1") == 1
            msgs = await _wait_done(sdk, source, dup_id)
            assert msgs["result"]["duplicates"] == 1

            # update then close
            r = await ext.post(
                f"/inbound/{source}",
                json={
                    "events": [
                        _event("e2", "account.updated", accountId="A1", status="restricted"),
                        _event("e3", "account.closed", accountId="A1"),
                    ]
                },
                headers={"X-Hook-Secret": secret},
            )
            assert r.status_code == 202
            await cb.wait_for(lambda: len(cb.of("delete")) == 1, "update + delete webhooks")
            _, upd, changed = cb.of("update")[0]
            assert changed == ["status"] and upd.data["status"] == "restricted"
            assert cb.of("delete")[0][1].version == 3
        await sdk.unsubscribe(handle)
    finally:
        await sdk.delete_inbound_source(source)


# I2 ------------------------------------------------------------------------------------------
async def test_i2_signed_publish_only_source(sdk: DBPersistClient) -> None:
    local_type = unique("ExtQuote")
    await sdk.register_type(local_type, ["symbol"])
    source = unique("quotes").lower()
    await sdk.create_inbound_source(
        source,
        {"type": f"const:{local_type}", "default_type": local_type, "external_id": "seq"},
        {
            "mode": "hmac_sha256",
            "secret": "k",
            "header": "X-Signature",
            "signature_prefix": "sha256=",
            "timestamp_header": "X-Timestamp",
            "signed_payload": "{timestamp}.{body}",
        },
        mode="publish",
        processing="sync",
    )
    cb = Collector()
    try:
        handle = await sdk.subscribe(
            local_type, {"field": "bid", "op": "gt", "value": 0}, callbacks=cb
        )
        raw = json.dumps({"seq": "7", "symbol": "ACME", "bid": 10.5}).encode()
        ts = str(int(time.time()))
        sig = "sha256=" + hmac.new(b"k", f"{ts}.".encode() + raw, hashlib.sha256).hexdigest()
        async with httpx.AsyncClient(base_url=f"{PERSIST_URL}/v1", timeout=30) as ext:
            bad = await ext.post(
                f"/inbound/{source}",
                content=raw,
                headers={"X-Signature": "sha256=0", "X-Timestamp": ts},
            )
            assert bad.status_code == 401
            ok = await ext.post(
                f"/inbound/{source}",
                content=raw,
                headers={"X-Signature": sig, "X-Timestamp": ts, "Content-Type": "application/json"},
            )
            assert ok.status_code == 200, ok.text
        await cb.wait_for(lambda: "ACME" in cb.keys("update"), "publish-only webhook")
        assert cb.of("update")[0][1].id is None
        assert await sdk.count(local_type) == 0
        await sdk.unsubscribe(handle)
    finally:
        await sdk.delete_inbound_source(source)


# I3 ------------------------------------------------------------------------------------------
async def test_i3_dry_run_and_management(sdk: DBPersistClient) -> None:
    source = unique("dry").lower()
    await sdk.create_inbound_source(source, _mapping("Nope_" + source), {"mode": "none"})
    try:
        out = await sdk.test_inbound_source(
            source,
            {"events": [_event("x", "account.opened", accountId="Q1"), _event("y", "other.kind")]},
        )
        assert out["auth_ok"] is None and len(out["events"]) == 1 and len(out["problems"]) == 1
        assert out["unknown_types"] == ["Nope_" + source]
        names = [s["name"] for s in await sdk.list_inbound_sources()]
        assert source in names
    finally:
        await sdk.delete_inbound_source(source)
