"""Stored API keys: table mixin, store, and the ``/api-keys`` admin router both services mount.

Each service keeps its own ``api_keys`` table (its database is its own); the SDK's
``create_api_key`` registers the same secret in both so one key works platform-wide.
"""

import uuid
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import Boolean, DateTime, Text, func, select
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Mapped, mapped_column

from dbpersist_common.auth import (
    Principal,
    PrincipalCache,
    Scope,
    get_principal,
    hash_key,
    new_key,
    require_admin,
)


class ApiKeyMixin:
    """Columns of the ``api_keys`` table; each service subclasses it with its own ``Base``."""

    __tablename__ = "api_keys"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    key_hash: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    key_hint: Mapped[str] = mapped_column(Text, nullable=False)  # last 4 chars, for display
    scopes: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ApiKeyIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    scopes: list[Scope] = Field(min_length=1)
    key: str | None = Field(
        default=None,
        min_length=32,
        description="register an existing secret (to share one key across services); "
        "omitted, a new one is generated",
    )


class ApiKeyOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    key_hint: str
    scopes: list[Scope]
    enabled: bool
    created_at: datetime
    last_used_at: datetime | None = None


class ApiKeyCreated(ApiKeyOut):
    key: str


class SqlKeyStore:
    """Resolves hashed keys from a service's ``api_keys`` table and stamps ``last_used_at``.

    ``last_used_at`` is written on cache misses only, so it is accurate to the cache TTL.
    """

    def __init__(self, maker: async_sessionmaker[AsyncSession], model: type[Any]) -> None:
        self._maker = maker
        self._model = model

    async def lookup(self, key_hash: str) -> Principal | None:
        async with self._maker() as session:
            row = (
                await session.execute(select(self._model).where(self._model.key_hash == key_hash))
            ).scalar_one_or_none()
            if row is None or not row.enabled:
                return None
            row.last_used_at = datetime.now(UTC)
            await session.commit()
            return Principal(name=row.name, scopes=[Scope.model_validate(s) for s in row.scopes])


def install_key_store(
    app_state: Any, maker: async_sessionmaker[AsyncSession], model: type[Any], ttl_seconds: float
) -> None:
    app_state.key_store = SqlKeyStore(maker, model)
    app_state.principal_cache = PrincipalCache(ttl_seconds)


def make_router(
    model: type[Any], get_session: Callable[..., AsyncIterator[AsyncSession]]
) -> APIRouter:
    """``/api-keys``: create (secret shown once), list (masked), disable, enable, delete.

    Every call needs a platform admin. A disabled or deleted key stops working in this process
    at once and in other processes within the principal-cache TTL.
    """
    router = APIRouter(prefix="/api-keys", tags=["api-keys"])

    def _invalidate(request: Request) -> None:
        cache: PrincipalCache | None = getattr(request.app.state, "principal_cache", None)
        if cache:
            cache.clear()

    async def _get(session: AsyncSession, key_id: uuid.UUID) -> Any:
        row = await session.get(model, key_id)
        if row is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "api key not found")
        return row

    @router.post("", response_model=ApiKeyCreated, status_code=status.HTTP_201_CREATED)
    async def create_key(
        body: ApiKeyIn,
        session: AsyncSession = Depends(get_session),
        principal: Principal = Depends(get_principal),
    ) -> ApiKeyCreated:
        require_admin(principal)
        secret = body.key or new_key()
        key_hash = hash_key(secret)
        existing = (
            await session.execute(
                select(model).where((model.name == body.name) | (model.key_hash == key_hash))
            )
        ).scalar_one_or_none()
        if existing is not None:
            what = "name" if existing.name == body.name else "key"
            raise HTTPException(status.HTTP_409_CONFLICT, f"api key with this {what} exists")
        row = model(
            name=body.name,
            key_hash=key_hash,
            key_hint=secret[-4:],
            scopes=[s.model_dump() for s in body.scopes],
        )
        session.add(row)
        await session.commit()
        return ApiKeyCreated(key=secret, **ApiKeyOut.model_validate(row).model_dump())

    @router.get("", response_model=list[ApiKeyOut])
    async def list_keys(
        session: AsyncSession = Depends(get_session),
        principal: Principal = Depends(get_principal),
    ) -> list[Any]:
        require_admin(principal)
        return list((await session.execute(select(model).order_by(model.name))).scalars())

    @router.post("/{key_id}/disable", response_model=ApiKeyOut)
    async def disable_key(
        key_id: uuid.UUID,
        request: Request,
        session: AsyncSession = Depends(get_session),
        principal: Principal = Depends(get_principal),
    ) -> Any:
        require_admin(principal)
        row = await _get(session, key_id)
        row.enabled = False
        await session.commit()
        _invalidate(request)
        return row

    @router.post("/{key_id}/enable", response_model=ApiKeyOut)
    async def enable_key(
        key_id: uuid.UUID,
        request: Request,
        session: AsyncSession = Depends(get_session),
        principal: Principal = Depends(get_principal),
    ) -> Any:
        require_admin(principal)
        row = await _get(session, key_id)
        row.enabled = True
        await session.commit()
        _invalidate(request)
        return row

    @router.delete("/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_key(
        key_id: uuid.UUID,
        request: Request,
        session: AsyncSession = Depends(get_session),
        principal: Principal = Depends(get_principal),
    ) -> None:
        require_admin(principal)
        row = await _get(session, key_id)
        await session.delete(row)
        await session.commit()
        _invalidate(request)

    return router
