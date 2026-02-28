from __future__ import annotations

import asyncio
import logging

import redis.asyncio as aioredis

from orchestrator.config import settings

logger = logging.getLogger(__name__)

_pool: aioredis.ConnectionPool | None = None
_pool_lock = asyncio.Lock()


async def get_pool() -> aioredis.ConnectionPool:
    """Retorna el pool compartido, creándolo de forma thread-safe si aún no existe."""
    global _pool
    if _pool is None:
        async with _pool_lock:
            # Double-checked locking: verificar de nuevo dentro del lock
            if _pool is None:
                _pool = aioredis.ConnectionPool.from_url(
                    settings.redis_url,
                    max_connections=20,
                    decode_responses=True,
                )
    return _pool


async def get_client() -> aioredis.Redis:
    return aioredis.Redis(connection_pool=await get_pool())


async def publish_progress(channel: str, payload: str) -> None:
    client = await get_client()
    await client.publish(channel, payload)


async def set_last_progress(task_id: str, payload: str, ttl_seconds: int = 3600) -> None:
    """Persiste el último estado de progreso para reconexiones del frontend."""
    client = await get_client()
    await client.setex(f"task:{task_id}:last_progress", ttl_seconds, payload)


async def get_last_progress(task_id: str) -> str | None:
    client = await get_client()
    return await client.get(f"task:{task_id}:last_progress")


async def ping() -> bool:
    try:
        client = await get_client()
        return await client.ping()
    except Exception:
        return False
