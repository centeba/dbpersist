"""API-key authentication and type-scoped authorization shared by both services.

A caller presents ``X-API-Key``. The key resolves to a :class:`Principal`: a name and a list of
scopes, each a type-name prefix with permissions. Routes then ask ``require_type`` whether the
principal may ``read``, ``write`` or ``admin`` the type they touch. Permissions nest: ``admin``
implies ``write`` implies ``read``. A scope whose prefix is ``*`` covers every type, and a
principal holding ``admin`` on ``*`` is a platform admin (type registry, inbound sources,
API keys, pub/sub admin).

Two kinds of key resolve:

* the bootstrap key from ``API_KEY`` in settings, a platform admin so existing deployments and
  the compose stack keep working; set it empty once real keys exist;
* keys stored (hashed) by each service in its ``api_keys`` table, looked up through the
  :class:`KeyStore` the app installs on ``app.state.key_store`` (see ``apikeys.py``).
"""

import contextvars
import hashlib
import logging
import secrets
import time
from typing import Literal, Protocol

from fastapi import Depends, Header, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

log = logging.getLogger(__name__)

Permission = Literal["read", "write", "admin"]
_RANK: dict[str, int] = {"read": 0, "write": 1, "admin": 2}
KEY_PREFIX = "dbp_"

current_principal: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "dbpersist_principal", default=None
)


class Scope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type_prefix: str = Field(min_length=1, max_length=200)
    permissions: list[Permission] = Field(min_length=1)

    @field_validator("type_prefix")
    @classmethod
    def _no_spaces(cls, v: str) -> str:
        if any(c.isspace() for c in v):
            raise ValueError("type_prefix must not contain whitespace")
        return v

    def covers(self, type_name: str) -> bool:
        return self.type_prefix == "*" or type_name.startswith(self.type_prefix)

    def grants(self, permission: Permission) -> bool:
        return any(_RANK[p] >= _RANK[permission] for p in self.permissions)


class Principal(BaseModel):
    name: str
    scopes: list[Scope]

    def allows(self, type_name: str, permission: Permission) -> bool:
        return any(s.covers(type_name) and s.grants(permission) for s in self.scopes)

    @property
    def is_admin(self) -> bool:
        return any(s.type_prefix == "*" and s.grants("admin") for s in self.scopes)

    def readable_types(self, names: list[str]) -> list[str]:
        return [n for n in names if self.allows(n, "read")]


ADMIN_SCOPES = [Scope(type_prefix="*", permissions=["admin"])]


class KeyStore(Protocol):
    async def lookup(self, key_hash: str) -> Principal | None: ...


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def new_key() -> str:
    return KEY_PREFIX + secrets.token_urlsafe(32)


class PrincipalCache:
    """Per-process cache of resolved keys, so a hot caller costs one lookup per TTL."""

    def __init__(self, ttl_seconds: float) -> None:
        self.ttl = ttl_seconds
        self._items: dict[str, tuple[Principal, float]] = {}

    def get(self, key_hash: str) -> Principal | None:
        hit = self._items.get(key_hash)
        if hit is None:
            return None
        principal, expires = hit
        if expires < time.monotonic():
            self._items.pop(key_hash, None)
            return None
        return principal

    def put(self, key_hash: str, principal: Principal) -> None:
        self._items[key_hash] = (principal, time.monotonic() + self.ttl)

    def clear(self) -> None:
        self._items.clear()


async def get_principal(
    request: Request, x_api_key: str | None = Header(default=None)
) -> Principal:
    """FastAPI dependency: resolve the API key or answer 401. Resolved once per request."""
    if not x_api_key:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing X-API-Key")
    settings = request.app.state.settings
    bootstrap: str = settings.api_key
    if bootstrap and secrets.compare_digest(x_api_key, bootstrap):
        principal = Principal(name="bootstrap", scopes=ADMIN_SCOPES)
    else:
        key_hash = hash_key(x_api_key)
        cache: PrincipalCache | None = getattr(request.app.state, "principal_cache", None)
        principal = cache.get(key_hash) if cache else None
        if principal is None:
            store: KeyStore | None = getattr(request.app.state, "key_store", None)
            principal = await store.lookup(key_hash) if store else None
            if principal is None:
                raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid X-API-Key")
            if cache:
                cache.put(key_hash, principal)
    request.state.principal = principal
    current_principal.set(principal.name)
    return principal


# Kept for routers that only need "some valid key" (the dependency name reads better there).
require_api_key = get_principal


def require_type(principal: Principal, type_name: str, permission: Permission) -> None:
    if not principal.allows(type_name, permission):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"key {principal.name!r} lacks {permission} permission on type {type_name!r}",
        )


async def admin_only(principal: Principal = Depends(get_principal)) -> Principal:
    """Router-level dependency for operator endpoints."""
    require_admin(principal)
    return principal


def require_admin(principal: Principal) -> None:
    if not principal.is_admin:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, f"key {principal.name!r} is not a platform admin"
        )
