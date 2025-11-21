from typing import AsyncGenerator, Optional
from fastapi import HTTPException, status
from sqlalchemy.orm import sessionmaker as orm_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from app.config.config import config

# Async engine + session for use with async code (FastAPI async endpoints)
# Note: settings.ASYNC_DATABASE_URL should be like 'postgresql+asyncpg://user:pass@host/db'
# Only create engines if PostgreSQL is enabled for this service
async_engine: Optional[AsyncEngine] = None
async_session_local: Optional[orm_sessionmaker] = None

async_read_replica_engine: Optional[AsyncEngine] = None
async_read_replica_session_local: Optional[orm_sessionmaker] = None

if config.IS_POSTGRES_ENABLED:
    async_engine = create_async_engine(
        config.ASYNC_DATABASE_URL, pool_pre_ping=True, future=True
    )
    async_session_local = orm_sessionmaker(
        bind=async_engine, class_=AsyncSession, expire_on_commit=False, future=True
    )

    # Async read replica engine + session
    async_read_replica_engine = create_async_engine(
        config.ASYNC_READ_DATABASE_URL, pool_pre_ping=True, future=True
    )
    async_read_replica_session_local = orm_sessionmaker(
        bind=async_read_replica_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        future=True,
    )


async def get_async_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield an AsyncSession for use in async endpoints/dependencies.

    Uses read replica if IS_USE_READ_REPLICA config is True, otherwise uses primary database.

    Raises:
        HTTPException: If PostgreSQL is disabled for this service (503 Service Unavailable)
    """
    # Check if PostgreSQL is enabled for this service
    if not config.IS_POSTGRES_ENABLED:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "PostgreSQL is disabled for this service. "
                "Set IS_POSTGRES_ENABLED=True to use database operations."
            ),
        )

    if not async_session_local:
        raise RuntimeError(
            "Database session not initialized. Ensure IS_POSTGRES_ENABLED=True "
            "and database configuration is correct."
        )

    if config.IS_USE_READ_REPLICA:
        if not async_read_replica_session_local:
            raise RuntimeError(
                "Read replica session not initialized. Ensure IS_POSTGRES_ENABLED=True "
                "and read replica configuration is correct."
            )
        async with async_read_replica_session_local() as session:
            yield session
    else:
        async with async_session_local() as session:
            yield session
