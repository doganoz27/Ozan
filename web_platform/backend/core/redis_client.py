"""
TITAN PRIME — Redis Client
"""
import json
from typing import Any, Optional

import redis.asyncio as aioredis

from core.config import settings

_redis: Optional[aioredis.Redis] = None


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = await aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
        )
    return _redis


async def redis_set(key: str, value: Any, ex: int = 300) -> None:
    r = await get_redis()
    await r.set(key, json.dumps(value), ex=ex)


async def redis_get(key: str) -> Optional[Any]:
    r = await get_redis()
    raw = await r.get(key)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return raw


async def redis_delete(key: str) -> None:
    r = await get_redis()
    await r.delete(key)


async def redis_publish(channel: str, message: Any) -> None:
    r = await get_redis()
    await r.publish(channel, json.dumps(message))


async def close_redis() -> None:
    global _redis
    if _redis:
        await _redis.aclose()
        _redis = None
