"""Bring an external publisher's event notifications into the order desk.

The publisher-specific part is a JSON file (see ``inbound/clearing-firm.json``): who signs
how, and how their envelope maps onto the desk's ``Customer`` and ``Order`` types. This module
only loads such a file, resolves ``${ENV_VAR}`` placeholders, and talks to dbpersist.

Commands
    register FILE            create or replace the inbound source described by FILE
    test FILE SAMPLE         dry-run SAMPLE (a JSON file) against the source: mapping + auth check
    simulate FILE SAMPLE     act as the publisher: POST SAMPLE to the ingress, signed per FILE
    status FILE              show queue counts, counters and the latest messages
    remove FILE              delete the source

Every event that gets through shows up in the dashboard exactly like a local change, because
the processor turns it into ordinary objects and events.
"""

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import httpx

from dbpersist_sdk import DBPersistClient, DBPersistError
from orderdesk import config

_PLACEHOLDER = re.compile(r"\$\{([A-Z0-9_]+)\}")


def _resolve(value: Any) -> Any:
    """Replace ${NAME} with os.environ[NAME]; an unset name becomes the empty string."""
    if isinstance(value, str):
        return _PLACEHOLDER.sub(lambda m: os.environ.get(m.group(1), ""), value)
    if isinstance(value, dict):
        return {k: _resolve(v) for k, v in value.items() if k != "_comment"}
    if isinstance(value, list):
        return [_resolve(v) for v in value]
    return value


def load(path: str) -> dict[str, Any]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    cfg = _resolve(raw)
    # the only placeholder that must resolve: the sender's secret (unless auth is "none")
    if cfg["auth"].get("mode") != "none" and not cfg["auth"].get("secret"):
        sys.exit("auth.secret is empty: set the environment variable the config references")
    # a pull block whose URL did not resolve is simply not configured
    if not cfg.get("pull", {}).get("url"):
        cfg.pop("pull", None)
    return cfg


def _admin() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=f"{config.PERSIST_URL}/v1", headers={"X-API-Key": config.API_KEY}, timeout=30
    )


async def register(cfg: dict[str, Any]) -> None:
    async with DBPersistClient(**config.client_kwargs()) as c:
        try:
            out = await c.create_inbound_source(
                cfg["name"],
                cfg["mapping"],
                cfg["auth"],
                mode=cfg.get("mode", "upsert"),
                update_mode=cfg.get("update_mode", "merge"),
                auto_register_types=cfg.get("auto_register_types", False),
                processing=cfg.get("processing", "queue"),
                pull=cfg.get("pull"),
                description=cfg.get("description", ""),
                enabled=cfg.get("enabled", True),
            )
            verb = "created"
        except DBPersistError as exc:
            if exc.status != 409:
                raise
            async with _admin() as a:
                r = await a.put(f"/inbound-sources/{cfg['name']}", json=cfg)
                r.raise_for_status()
                out = r.json()
            verb = "updated"
    print(f"{verb} inbound source {out['name']}: POST {config.PERSIST_URL}{out['ingest_path']}")
    print(
        f"  mode={out['mode']} processing={out['processing']} auth={out['auth']['mode']} "
        f"pull={'on' if out['pull'] and out['pull']['enabled'] else 'off'}"
    )


async def test(cfg: dict[str, Any], sample_path: str) -> None:
    sample = json.loads(Path(sample_path).read_text(encoding="utf-8"))
    body = json.dumps(sample, separators=(",", ":"))
    headers = sign(cfg["auth"], body.encode())
    async with DBPersistClient(**config.client_kwargs()) as c:
        out = await c.test_inbound_source(cfg["name"], sample, headers)
    # the SDK test call uses json.dumps(payload) server-side; send the exact body we signed
    async with _admin() as a:
        r = await a.post(
            f"/inbound-sources/{cfg['name']}/test",
            json={"payload": sample, "headers": headers, "body": body},
        )
        r.raise_for_status()
        out = r.json()
    print(f"auth check: {'ok' if out['auth_ok'] else out['auth_error']}")
    for e in out["events"]:
        print(
            f"  #{e['index']} {e['external_type']:<20} -> {e['type_name']:<10} {e['op']:<6} "
            f"key={e['key_values']} ext_id={e['external_id']}"
        )
    for p in out["problems"]:
        print(f"  #{p['index']} REJECTED: {p['reason']}")
    if out["unknown_types"]:
        print(f"  types not registered yet: {out['unknown_types']}")


def sign(auth: dict[str, Any], body: bytes) -> dict[str, str]:
    """Produce the headers the publisher would send, for the configured scheme."""
    mode = auth["mode"]
    secret = auth.get("secret", "")
    if mode == "none":
        return {}
    if mode == "header_secret":
        return {auth.get("header", "X-Webhook-Secret"): secret}
    if mode == "bearer":
        return {"Authorization": f"Bearer {secret}"}
    if mode == "basic":
        cred = base64.b64encode(f"{auth['username']}:{secret}".encode()).decode()
        return {"Authorization": f"Basic {cred}"}
    if mode == "hmac_sha256":
        ts = str(int(time.time()))
        signed = auth.get("signed_payload", "{body}").replace("{timestamp}", ts).encode()
        signed = signed.replace(b"{body}", body)
        mac = hmac.new(secret.encode(), signed, hashlib.sha256).digest()
        value = (
            mac.hex() if auth.get("encoding", "hex") == "hex" else base64.b64encode(mac).decode()
        )
        headers = {auth.get("header", "X-Signature"): auth.get("signature_prefix", "") + value}
        if auth.get("timestamp_header"):
            headers[auth["timestamp_header"]] = ts
        return headers
    sys.exit(f"simulate does not support auth mode {mode!r}")


async def simulate(cfg: dict[str, Any], sample_path: str) -> None:
    body = json.dumps(
        json.loads(Path(sample_path).read_text(encoding="utf-8")), separators=(",", ":")
    ).encode()
    headers = {**sign(cfg["auth"], body), "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=30) as http:  # no API key: we are the publisher
        r = await http.post(
            f"{config.PERSIST_URL}/v1/inbound/{cfg['name']}", content=body, headers=headers
        )
    print(f"{r.status_code} {r.text}")


async def status(cfg: dict[str, Any]) -> None:
    async with _admin() as a:
        src = (await a.get(f"/inbound-sources/{cfg['name']}")).json()
        msgs = (await a.get(f"/inbound-sources/{cfg['name']}/messages?limit=5")).json()
    print(
        f"{src['name']}: queue={src['queue']} received={src['received_count']} "
        f"accepted={src['accepted_count']} duplicates={src['duplicate_count']} "
        f"rejected={src['rejected_count']}"
    )
    if src.get("pull"):
        print(
            f"  pull: cursor={src['pull_cursor']} last={src['last_pulled_at']} "
            f"error={src['last_pull_error']}"
        )
    for m in msgs:
        summary = m["result"] and {
            k: m["result"][k] for k in ("accepted", "duplicates", "rejected")
        }
        print(
            f"  message {m['id']} {m['origin']} {m['status']} attempts={m['attempts']} "
            f"{summary or m['last_error'] or ''}"
        )


async def remove(cfg: dict[str, Any]) -> None:
    async with DBPersistClient(**config.client_kwargs()) as c:
        await c.delete_inbound_source(cfg["name"])
    print(f"removed {cfg['name']}")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        prog="orderdesk.inbound",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("register", "status", "remove"):
        s = sub.add_parser(name)
        s.add_argument("file")
    for name in ("test", "simulate"):
        s = sub.add_parser(name)
        s.add_argument("file")
        s.add_argument("sample")
    a = p.parse_args(argv)
    cfg = load(a.file)

    async def run() -> None:
        match a.cmd:
            case "register":
                await register(cfg)
            case "test":
                await test(cfg, a.sample)
            case "simulate":
                await simulate(cfg, a.sample)
            case "status":
                await status(cfg)
            case "remove":
                await remove(cfg)

    asyncio.run(run())


if __name__ == "__main__":
    main()
