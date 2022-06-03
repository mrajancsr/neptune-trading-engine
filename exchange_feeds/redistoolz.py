import os
from enum import Enum
from json import loads
from typing import Optional, TypeVar

import aioredis

Blotter = TypeVar("Blotter")
Book = TypeVar("Book")
LOCALHOST: str = "redis://localhost"
DOCKERHOST: str = "redis://myredis"
WRITERHOST: str = os.environ.get("WRITERHOST", LOCALHOST)
READERHOST: str = os.environ.get("READERHOST", LOCALHOST)
EXECUTE_IN_DOCKER: Optional[bool] = loads(
    os.environ.get("EXECUTE_IN_DOCKER", "False").lower()
)
EXECUTE_LOCALLY: Optional[bool] = loads(
    os.environ.get("EXECUTE_LOCALLY", "False").lower()
)
REDISPORT: int = int(os.environ.get("REDISPORT", "6379"))


class RedisActionType(Enum):
    WRITE_ONLY = "writeonly"
    READ_ONLY = "readonly"


async def connect_to_redis(
    action: RedisActionType = RedisActionType.READ_ONLY,
):
    # covers local execution
    if EXECUTE_LOCALLY and action == RedisActionType.READ_ONLY:
        redis = await aioredis.from_url(
            LOCALHOST, port=REDISPORT, decode_responses=True
        )
    elif EXECUTE_LOCALLY and action == RedisActionType.WRITE_ONLY:
        redis = await aioredis.from_url(LOCALHOST, port=REDISPORT)

    # covers docker execution only for testing purposses
    elif EXECUTE_IN_DOCKER and action == RedisActionType.READ_ONLY:
        redis = await aioredis.from_url(
            DOCKERHOST, port=REDISPORT, decode_responses=True
        )
    elif EXECUTE_IN_DOCKER and action == RedisActionType.WRITE_ONLY:
        redis = await aioredis.from_url(DOCKERHOST, port=REDISPORT)

    # covers kubernetees
    elif action == RedisActionType.READ_ONLY:
        redis = await aioredis.from_url(
            READERHOST, port=REDISPORT, decode_responses=True
        )
    elif action == RedisActionType.WRITE_ONLY:
        redis = await aioredis.from_url(WRITERHOST, port=REDISPORT)

    if await redis.ping():
        print("connected to Redis!")
    return redis
