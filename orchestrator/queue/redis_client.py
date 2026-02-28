from __future__ import annotations

import logging

import redis.asyncio as aioredis

from orchestrator.config import settings

logger = logging.getLogger(__name__)

_pool: aioredis.ConnectionPool | None = None


def get_pool() -> aioredis.ConnectionPool:
    global _pool
    if _pool is None:
        _pool = aioredis.ConnectionPool.from_url(
            settings.redis_url,
            max_connections=20,
            decode_responses=True,
        )
    return _pool


def get_client() -> aioredis.Redis:
    return aioredis.Redis(connection_pool=get_pool())


async def publish_progress(channel: str, payload: str) -> None:
    client = get_client()
    await client.publish(channel, payload)


async def set_last_progress(task_id: str, payload: str, ttl_seconds: int = 3600) -> None:
    """Persiste el último estado de progreso para reconexiones del frontend."""
    client = get_client()
    await client.setex(f"task:{task_id}:last_progress", ttl_seconds, payload)


async def get_last_progress(task_id: str) -> str | None:
    client = get_client()
    return await client.get(f"task:{task_id}:last_progress")


async def ping() -> bool:
    try:
        client = get_client()
        return await client.ping()
    except Exception:
        return False
