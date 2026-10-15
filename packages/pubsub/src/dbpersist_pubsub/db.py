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
    # No pool_pre_ping: it adds a round trip per checkout and every task that hits a stale
    # connection (e.g. after a Postgres restart) already fails into Celery's retry path.
    # pool_recycle still drops connections older than 30 minutes.
    return create_engine(
        Settings().pubsub_sync_database_url, pool_size=10, max_overflow=30, pool_recycle=1800
    )


@lru_cache(maxsize=1)
def autocommit_engine() -> Engine:
    """The sync engine (same pool) in AUTOCOMMIT mode: no BEGIN/COMMIT round trips.

    Used by the hot task paths, which issue one or two self-contained statements each.
    """
    return sync_engine().execution_options(isolation_level="AUTOCOMMIT")


@lru_cache(maxsize=1)
def sync_sessionmaker() -> sessionmaker[Session]:
    return sessionmaker(sync_engine(), expire_on_commit=False)


@lru_cache(maxsize=1)
def autocommit_sessionmaker() -> sessionmaker[Session]:
    return sessionmaker(autocommit_engine(), expire_on_commit=False)
