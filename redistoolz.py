import asyncio
import os
from asyncio.events import AbstractEventLoop
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from json import loads
from multiprocessing.managers import BaseManager
from typing import Any, Dict, Iterator, List, Optional, Set, TypeVar, Union

import aioredis
import websockets
from aioredis import Redis
from aioredis.client import Pipeline

from exchange_feeds.constants import L2_ENABLED_STREAMS, STREAM_NAMES, RedisActionType
from limit_order_book.book import LimitOrderBook

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


MAX_RECORD_COUNT = 2


class OrderBookManager(BaseManager):
    pass


def BookManager():
    m = OrderBookManager()
    m.start()
    return m


# -- uncomment to use multiprocessing base manager
# OrderBookManager.register("LimitOrderBook", LimitOrderBook)


@dataclass
class Feed:
    # - Stores the stream name and the raw feeds every 15 minutes
    stream_name: str
    records: List[Dict[str, Any]]
    column_names: Optional[List[str]] = field(init=False, default=list)

    def __post_init__(self) -> None:
        self.column_names = self.get_column_names()

    def record_count(self) -> int:
        return len(self.records)

    def get_records(self) -> Iterator:
        return (tuple(item.values()) for _, item in self.records)

    def get_ids(self) -> Iterator:
        return (id for id, _ in self.records)

    def get_column_names(self) -> Optional[List[str]]:
        if self.record_count() == 0:
            return []
        return list(self.records[0][1].keys())


async def connect_to_redis(action: RedisActionType = RedisActionType.READ_ONLY):
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


async def process_records(
    records: List[Dict],
    stream_name: str,
    handle_lob: bool,
    symbol: str,
    book: LimitOrderBook,
):
    with ThreadPoolExecutor() as thread_pool:
        loop: AbstractEventLoop = asyncio.get_running_loop()
        calls: List[partial[Dict]] = [
            partial(handle_record, record, stream_name, handle_lob, symbol, book)
            for record in records
        ]
        call_coros = []
        for call in calls:
            call_coros.append(loop.run_in_executor(thread_pool, call))

        results = await asyncio.gather(*call_coros)
        for record in results:
            yield record


async def read_feed_from_redis(
    stream_name: str, redis: Optional[Redis] = None
) -> Optional[Feed]:
    """Reads a single feed from redis
    Parameters
    ----------
    stream_name : str
        the exchange stream name
    redis : Optional[Redis], default=None
        connection from localhost,
    Returns
    -------
    Optional[Feed]
        contains stream name and raw feed
    """
    if not redis:
        redis = await connect_to_redis(action=RedisActionType.READ_ONLY)
    record_count = await redis.xlen(stream_name)
    if not record_count:
        return None
    print(f"reading from {stream_name} started")

    records = await redis.xrange(stream_name)

    return Feed(stream_name, records)


async def push_feed_to_postgres(feed: Feed) -> None:
    # db = DBReader()
    if feed.record_count() == 0:
        print(f"feed {feed.stream_name} is empty, nothing to push")
        return
    table_name = feed.stream_name.replace("-", "_")
    column_names = feed.column_names
    if column_names:
        print(f"pushing data from {feed.stream_name} to {table_name}")
        db.push(feed.get_records(), table_name, column_names)
        print(f"push to {table_name} successfull")
    else:
        print(f"failed to push to {table_name} due to incorrect column names")


def recursivels(
    parent_path: str,
    files: List[str],
    exclusions: Set[str],
) -> None:
    """Recursively get all files names in phobos path
    Parameters
    ----------
    parent_path : str
        parent directory where folders are located
    files : list
        stores the files names in list
    """
    if os.path.isfile(parent_path) and parent_path not in exclusions:
        files.append(parent_path)
    elif os.path.isdir(parent_path) and not parent_path.startswith("."):
        for entry in os.listdir(parent_path):
            if (
                entry.startswith("__")
                or entry.startswith(".")
                or entry.endswith(".ipynb")
                or entry in exclusions
            ):
                continue
            subdir = os.path.join(parent_path, entry)
            recursivels(subdir, files, exclusions)
