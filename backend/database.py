from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from backend.config import get_settings
import redis.asyncio as aioredis

settings = get_settings()


def _pg_connect_args(database_url: str) -> dict:
    """SSL for managed Postgres (Heroku). Local dev needs no TLS."""
    if "localhost" in database_url or "127.0.0.1" in database_url:
        return {}
    # Heroku documents sslmode=require for external clients. asyncpg accepts the
    # string "require" directly (encrypts, ignores cert chain errors). Do NOT use
    # ssl=True here — that maps to verify-full and fails on CI runners.
    return {"ssl": "require"}


engine = create_async_engine(
    settings.async_database_url,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    connect_args=_pg_connect_args(settings.database_url),
)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

_redis: aioredis.Redis | None = None


def _redis_client(url: str) -> aioredis.Redis:
    """Heroku Redis uses TLS (rediss://) with a self-signed chain."""
    kwargs: dict = {"decode_responses": True}
    if url.startswith("rediss://") or (
        "localhost" not in url and "127.0.0.1" not in url
    ):
        # Heroku docs: ssl_cert_reqs=None disables cert validation for self-signed TLS.
        kwargs["ssl_cert_reqs"] = None
        kwargs["ssl_check_hostname"] = False
    return aioredis.from_url(url, **kwargs)


def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = _redis_client(settings.redis_url)
    return _redis


class Base(DeclarativeBase):
    pass


async def init_db():
    from backend.models import Base as ModelBase  # noqa: F401
    async with engine.begin() as conn:
        pass  # Alembic manages schema; this just verifies connection


async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
