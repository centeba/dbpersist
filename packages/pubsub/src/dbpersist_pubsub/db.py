"""Two engines over one database: async for the FastAPI side, sync for Celery prefork workers."""

from collections.abc import AsyncIterator
from functools import lru_cache

from fastapi import Request
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from dbpersist_pubsub.settings import Settings


class Base(DeclarativeBase):
    pass


def make_async_engine(url: str) -> AsyncEngine:
    return create_async_engine(url, pool_size=10, max_overflow=10)


def make_async_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    maker: async_sessionmaker[AsyncSession] = request.app.state.sessionmaker
    async with maker() as session:
        yield session


@lru_cache(maxsize=1)
def sync_engine() -> Engine:
    return create_engine(
        Settings().pubsub_sync_database_url, pool_size=5, max_overflow=5, pool_pre_ping=True
    )


@lru_cache(maxsize=1)
def sync_sessionmaker() -> sessionmaker[Session]:
    return sessionmaker(sync_engine(), expire_on_commit=False)
