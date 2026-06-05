"""
storage/db.py
──────────────
SQLAlchemy async engine + session factory.

One engine per process. Sessions are created per-request via
get_session() — an async context manager.

M11 additions:
  - pgvector asyncpg codec registration on each new connection
  - init_db() — creates all tables (development/CI convenience)
    Production should use Alembic migrations instead.

Usage:
    async with get_session() as session:
        result = await session.execute(select(RunRecord))

    # One-time setup (dev):
    await init_db()
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import lru_cache

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from chive_shared.logging import get_logger
from chive_shared.settings import get_storage_settings

logger = get_logger(__name__)


@lru_cache(maxsize=1)
def get_engine() -> AsyncEngine:
    """
    Return the singleton async engine.

    Registers the pgvector asyncpg codec on every new connection so that
    Vector columns are correctly serialised/deserialised without explicit casts.
    """
    settings = get_storage_settings()
    engine = create_async_engine(
        settings.database_url,
        echo=False,
        pool_size=10,
        max_overflow=5,
    )
    # pgvector.sqlalchemy.Vector handles type serialisation natively via
    # SQLAlchemy's type system — no asyncpg-level codec registration needed.
    logger.info("db_engine_created", url=_masked_url(settings.database_url))
    return engine


def _get_session_factory() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        get_engine(),
        class_=AsyncSession,
        expire_on_commit=False,
    )


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager that yields a database session.

    Commits on clean exit, rolls back on exception.
    """
    factory = _get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def init_db() -> None:
    """
    Create all tables if they don't exist.

    Convenience for development and CI. Production deployments should use
    Alembic migrations (alembic upgrade head) for schema evolution without
    data loss.

    Requires the pgvector extension to be installed in Postgres:
        CREATE EXTENSION IF NOT EXISTS vector;
    """
    from chive_shared.storage.models import Base

    engine = get_engine()

    # Ensure pgvector extension is enabled (idempotent)
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Incremental column migrations — safe to run on existing DBs.
    # Each ALTER is idempotent (IF NOT EXISTS) so re-running init-db never breaks data.
    async with engine.begin() as conn:
        await conn.execute(text(
            "ALTER TABLE api_calls ADD COLUMN IF NOT EXISTS result_count INTEGER DEFAULT NULL"
        ))

    logger.info("db_tables_created")


def _masked_url(url: str) -> str:
    """Hide password in database URL for logging."""
    if "@" in url:
        scheme, rest = url.split("://", 1)
        credentials, host = rest.split("@", 1)
        user = credentials.split(":")[0]
        return f"{scheme}://{user}:***@{host}"
    return url
