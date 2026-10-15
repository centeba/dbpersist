"""Async client facade over the persistence and pubsub services.

Offers select / active_select / transactions / publish_only / subscribe and keeps a
small identity cache keyed by (type, key).
"""

import uuid
from collections.abc import AsyncIterator
from types import TracebackType
from typing import Any, Literal

import httpx
from pydantic import ValidationError

from dbpersist_common.filters import dump_filter, parse_filter
from dbpersist_common.inbound import InboundAuth, InboundMapping, InboundPull
from dbpersist_sdk.exceptions import (
    DBPersistError,
    DuplicateKey,
    InvalidData,
    NotFound,
    Unauthorized,
    VersionConflict,
)
from dbpersist_sdk.models import (
    Callbacks,
    ObjectRecord,
    ObjectTypeInfo,
    SubscriberInfo,
    SubscriptionHandle,
    TransactionResult,
)

UpdateMode = Literal["replace", "merge"]
FilterLike = dict[str, Any] | None

_ERRORS: dict[str, type[DBPersistError]] = {
    "not_found": NotFound,
    "duplicate_key": DuplicateKey,
    "version_conflict": VersionConflict,
    "invalid_data": InvalidData,
}


def _raise_for(resp: httpx.Response) -> None:
    if resp.is_success:
        return
    try:
        payload = resp.json()
    except ValueError:
        payload = {"detail": resp.text}
    if not isinstance(payload, dict):
        payload = {"detail": payload}
    code = payload.get("code")
    detail = payload.get("detail", resp.reason_phrase)
    if resp.status_code == 401:
        raise Unauthorized(str(detail), resp.status_code, payload)
    if resp.status_code == 404 and code is None:
        raise NotFound(str(detail), resp.status_code, payload)
    if resp.status_code == 422 and code is None:
        raise InvalidData(str(detail), resp.status_code, payload)
    cls = _ERRORS.get(code or "", DBPersistError)
    raise cls(str(detail), resp.status_code, payload)


def _norm_filter(f: FilterLike) -> dict[str, Any] | None:
    if f is None:
        return None
    try:
        return dump_filter(parse_filter(f))
    except ValidationError as exc:
        raise InvalidData(f"invalid filter: {exc.errors()[0].get('msg', exc)}", 422) from exc


class Transaction:
    """Collects ops; the outermost context exit commits them atomically.

    Nested ``async with tx.begin()`` blocks only increase a depth counter: there is one
    physical commit, when the depth returns to zero.
    """

    def __init__(self, client: DBPersistClient) -> None:
        self._client = client
        self.ops: list[dict[str, Any]] = []
        self.depth = 0
        self.result: TransactionResult | None = None

    def insert(self, type_name: str, data: dict[str, Any]) -> None:
        self.ops.append({"op": "insert", "type": type_name, "data": data})

    def update(
        self,
        type_name: str,
        obj_id: uuid.UUID | str,
        version: int,
        data: dict[str, Any],
        mode: UpdateMode = "replace",
    ) -> None:
        self.ops.append(
            {
                "op": "update",
                "type": type_name,
                "id": str(obj_id),
                "version": version,
                "data": data,
                "mode": mode,
            }
        )

    def delete(self, type_name: str, obj_id: uuid.UUID | str, version: int) -> None:
        self.ops.append({"op": "delete", "type": type_name, "id": str(obj_id), "version": version})

    def discard(self) -> None:
        self.ops.clear()

    def begin(self) -> Transaction:
        return self

    async def __aenter__(self) -> Transaction:
        self.depth += 1
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.depth -= 1
        if exc_type is not None:
            self.ops.clear()
            return
        if self.depth == 0 and self.ops:
            self.result = await self._client.commit(self.ops)
            self.ops = []


class DBPersistClient:
    def __init__(
        self,
        persist_url: str,
        pubsub_url: str,
        api_key: str,
        *,
        subscriber_name: str | None = None,
        webhook_url: str | None = None,
        cache: bool = False,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        headers = {"X-API-Key": api_key}
        self._p = httpx.AsyncClient(
            base_url=persist_url.rstrip("/") + "/v1",
            headers=headers,
            timeout=timeout,
            transport=transport,
        )
        self._s = httpx.AsyncClient(
            base_url=pubsub_url.rstrip("/") + "/v1",
            headers=headers,
            timeout=timeout,
            transport=transport,
        )
        self.subscriber_name = subscriber_name
        self.webhook_url = webhook_url
        self.subscriber_id: uuid.UUID | None = None
        self.secret: str | None = None
        self.callbacks: dict[uuid.UUID, Callbacks] = {}
        self.cache: dict[tuple[str, str], ObjectRecord] | None = {} if cache else None

    async def __aenter__(self) -> DBPersistClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._p.aclose()
        await self._s.aclose()

    # ---- cache -----------------------------------------------------------------------------

    def _remember(self, rec: ObjectRecord) -> ObjectRecord:
        if self.cache is not None:
            self.cache[(rec.type_name, rec.key)] = rec
        return rec

    def _forget(self, type_name: str, key: str) -> None:
        if self.cache is not None:
            self.cache.pop((type_name, key), None)

    # ---- persistence -----------------------------------------------------------------------

    async def register_type(
        self, name: str, key_fields: list[str], json_schema: dict[str, Any] | None = None
    ) -> ObjectTypeInfo:
        r = await self._p.post(
            "/types",
            json={"name": name, "key_fields": key_fields, "json_schema": json_schema or {}},
        )
        _raise_for(r)
        return ObjectTypeInfo.model_validate(r.json())

    async def get_type(self, name: str) -> ObjectTypeInfo:
        r = await self._p.get(f"/types/{name}")
        _raise_for(r)
        return ObjectTypeInfo.model_validate(r.json())

    async def insert(self, type_name: str, data: dict[str, Any]) -> ObjectRecord:
        r = await self._p.post(f"/objects/{type_name}", json=data)
        _raise_for(r)
        return self._remember(ObjectRecord.model_validate(r.json()))

    async def get(
        self, type_name: str, obj_id: uuid.UUID | str, expand: list[str] | None = None
    ) -> ObjectRecord:
        params = {"expand": ",".join(expand)} if expand else None
        r = await self._p.get(f"/objects/{type_name}/{obj_id}", params=params)
        _raise_for(r)
        return self._remember(ObjectRecord.model_validate(r.json()))

    async def get_by_key(
        self, type_name: str, *key_values: str, expand: list[str] | None = None
    ) -> ObjectRecord:
        params = {"expand": ",".join(expand)} if expand else None
        r = await self._p.get(f"/objects/{type_name}/by-key/" + "/".join(key_values), params=params)
        _raise_for(r)
        return self._remember(ObjectRecord.model_validate(r.json()))

    async def update(
        self,
        type_name: str,
        obj_id: uuid.UUID | str,
        version: int,
        data: dict[str, Any],
        mode: UpdateMode = "replace",
    ) -> ObjectRecord:
        method = self._p.put if mode == "replace" else self._p.patch
        r = await method(f"/objects/{type_name}/{obj_id}", json={"version": version, "data": data})
        _raise_for(r)
        return self._remember(ObjectRecord.model_validate(r.json()))

    async def delete(self, type_name: str, obj_id: uuid.UUID | str, version: int) -> None:
        r = await self._p.delete(f"/objects/{type_name}/{obj_id}", params={"version": version})
        _raise_for(r)
        if self.cache is not None:
            for k, rec in list(self.cache.items()):
                if rec.id is not None and str(rec.id) == str(obj_id):
                    self.cache.pop(k)

    async def select(
        self,
        type_name: str,
        filter: FilterLike = None,
        *,
        order_by: list[dict[str, Any]] | None = None,
        limit: int = 100,
        offset: int = 0,
        expand: list[str] | None = None,
    ) -> list[ObjectRecord]:
        body: dict[str, Any] = {"filter": _norm_filter(filter), "limit": limit, "offset": offset}
        if order_by:
            body["order_by"] = order_by
        if expand:
            body["expand"] = expand
        r = await self._p.post(f"/objects/{type_name}/query", json=body)
        _raise_for(r)
        return [self._remember(ObjectRecord.model_validate(i)) for i in r.json()["items"]]

    async def select_iter(
        self,
        type_name: str,
        filter: FilterLike = None,
        *,
        page_size: int = 500,
        expand: list[str] | None = None,
    ) -> AsyncIterator[ObjectRecord]:
        """Stream all matches by following the keyset cursor."""
        cursor: str | None = None
        f = _norm_filter(filter)
        while True:
            body: dict[str, Any] = {"filter": f, "limit": page_size}
            if cursor:
                body["cursor"] = cursor
            if expand:
                body["expand"] = expand
            r = await self._p.post(f"/objects/{type_name}/query", json=body)
            _raise_for(r)
            page = r.json()
            for item in page["items"]:
                yield self._remember(ObjectRecord.model_validate(item))
            cursor = page.get("next_cursor")
            if not cursor:
                return

    async def count(self, type_name: str, filter: FilterLike = None) -> int:
        r = await self._p.post(f"/objects/{type_name}/count", json={"filter": _norm_filter(filter)})
        _raise_for(r)
        return int(r.json()["count"])

    async def commit(self, ops: list[dict[str, Any]]) -> TransactionResult:
        r = await self._p.post("/transactions", json={"ops": ops})
        _raise_for(r)
        result = TransactionResult.model_validate(r.json())
        for res in result.results:
            if res.op == "delete":
                self._forget(res.object.type_name, res.object.key)
            else:
                self._remember(res.object)
        return result

    def transaction(self) -> Transaction:
        return Transaction(self)

    async def publish_only(
        self,
        type_name: str,
        op: Literal["insert", "update", "delete"],
        data: dict[str, Any],
        key: str | None = None,
        changed_fields: list[str] | None = None,
    ) -> uuid.UUID:
        body: dict[str, Any] = {
            "type": type_name,
            "op": op,
            "data": data,
            "changed_fields": changed_fields or [],
        }
        if key is not None:
            body["key"] = key
        r = await self._p.post("/publish", json=body)
        _raise_for(r)
        return uuid.UUID(r.json()["event_id"])

    # ---- pubsub ----------------------------------------------------------------------------

    def use_subscriber(self, subscriber_id: uuid.UUID | str, secret: str) -> None:
        """Adopt an existing registration (e.g. restored from config after a restart)."""
        self.subscriber_id = uuid.UUID(str(subscriber_id))
        self.secret = secret

    async def register_subscriber(self) -> SubscriberInfo:
        if not self.subscriber_name or not self.webhook_url:
            raise DBPersistError("subscriber_name and webhook_url are required to register")
        r = await self._s.post(
            "/subscribers", json={"name": self.subscriber_name, "webhook_url": self.webhook_url}
        )
        _raise_for(r)
        info = SubscriberInfo.model_validate(r.json())
        self.use_subscriber(info.id, info.secret)
        return info

    async def unregister_subscriber(self) -> None:
        if self.subscriber_id is None:
            return
        r = await self._s.delete(f"/subscribers/{self.subscriber_id}")
        _raise_for(r)
        self.subscriber_id = None
        self.secret = None
        self.callbacks.clear()

    async def heartbeat(self) -> None:
        self._require_subscriber()
        r = await self._s.post(f"/subscribers/{self.subscriber_id}/heartbeat")
        _raise_for(r)

    async def subscribe(
        self,
        type_name: str,
        filter: FilterLike = None,
        *,
        keys: list[str] | None = None,
        by: Literal["key", "id"] = "key",
        callbacks: Callbacks | None = None,
    ) -> SubscriptionHandle:
        self._require_subscriber()
        body: dict[str, Any] = {"type": type_name}
        if keys is not None:
            body["keys"] = keys
            body["by"] = by
        else:
            body["filter"] = _norm_filter(filter)
        r = await self._s.post(f"/subscribers/{self.subscriber_id}/subscriptions", json=body)
        _raise_for(r)
        handle = SubscriptionHandle.model_validate({**r.json(), "type_name": type_name})
        if callbacks is not None:
            self.callbacks[handle.id] = callbacks
        return handle

    async def unsubscribe(self, handle: SubscriptionHandle | uuid.UUID) -> None:
        sid = handle.id if isinstance(handle, SubscriptionHandle) else handle
        r = await self._s.delete(f"/subscriptions/{sid}")
        _raise_for(r)
        self.callbacks.pop(sid, None)

    async def active_select(
        self,
        type_name: str,
        filter: FilterLike,
        callbacks: Callbacks,
        **select_kwargs: Any,
    ) -> tuple[list[ObjectRecord], SubscriptionHandle]:
        """Subscribe first, then select, so no change between the two is missed."""
        handle = await self.subscribe(type_name, filter, callbacks=callbacks)
        rows = await self.select(type_name, filter, **select_kwargs)
        return rows, handle

    def _require_subscriber(self) -> None:
        if self.subscriber_id is None:
            raise DBPersistError(
                "not registered: call register_subscriber() or use_subscriber() first"
            )

    # ---- inbound sources (external senders -> dbpersist) ---------------------------------

    async def create_inbound_source(
        self,
        name: str,
        mapping: InboundMapping | dict[str, Any],
        auth: InboundAuth | dict[str, Any],
        *,
        mode: Literal["upsert", "publish"] = "upsert",
        update_mode: UpdateMode = "merge",
        auto_register_types: bool = False,
        processing: Literal["queue", "sync"] = "queue",
        pull: InboundPull | dict[str, Any] | None = None,
        description: str = "",
        enabled: bool = True,
    ) -> dict[str, Any]:
        """Register an external sender. Returns the source as the API shows it (secret masked).

        Point the sender at ``<persist_url>/v1/inbound/<name>``; that endpoint is authenticated
        by ``auth``, not by the dbpersist API key.
        """
        m = (
            mapping
            if isinstance(mapping, InboundMapping)
            else InboundMapping.model_validate(mapping)
        )
        a = auth if isinstance(auth, InboundAuth) else InboundAuth.model_validate(auth)
        r = await self._p.post(
            "/inbound-sources",
            json={
                "name": name,
                "description": description,
                "enabled": enabled,
                "mode": mode,
                "update_mode": update_mode,
                "auto_register_types": auto_register_types,
                "processing": processing,
                "auth": a.model_dump(),
                "mapping": m.model_dump(exclude_none=True),
                "pull": (
                    None
                    if pull is None
                    else (
                        pull if isinstance(pull, InboundPull) else InboundPull.model_validate(pull)
                    ).model_dump()
                ),
            },
        )
        _raise_for(r)
        return r.json()

    async def test_inbound_source(
        self, name: str, payload: Any, headers: dict[str, str] | None = None
    ) -> dict[str, Any]:
        """Dry run a sample payload (and optionally sample headers) against a source's mapping."""
        r = await self._p.post(
            f"/inbound-sources/{name}/test", json={"payload": payload, "headers": headers or {}}
        )
        _raise_for(r)
        return r.json()

    async def list_inbound_sources(self) -> list[dict[str, Any]]:
        r = await self._p.get("/inbound-sources")
        _raise_for(r)
        return r.json()

    async def delete_inbound_source(self, name: str) -> None:
        r = await self._p.delete(f"/inbound-sources/{name}")
        _raise_for(r)

    def inbound_url(self, name: str) -> str:
        return f"{self._p.base_url}/inbound/{name}"
