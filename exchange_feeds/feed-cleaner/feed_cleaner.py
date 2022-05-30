"""The purpose of this file is to clean the redis data streams in memory every
minute and then push the cleaned data to a historical database
"""
import asyncio
import os
from dataclasses import dataclass, field
from json import loads
from time import perf_counter
from typing import Any, Dict, Iterator, List, Optional

import aioredis
import uvloop

from metric_modifier import batch_records, push_feed_to_questdb

# event policy needs to be set at top of file
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

LOCALHOST: str = "redis://localhost"
DOCKERHOST: str = "redis://myredis"
WRITERHOST: str = os.environ.get("WRITERHOST", LOCALHOST)
READERHOST: str = os.environ.get("READERHOST", LOCALHOST)
EXECUTE_LOCALLY: Optional[bool] = loads(
    os.environ.get("EXECUTE_LOCALLY", "False").lower()
)
REDISPORT: int = int(os.environ.get("REDISPORT", "6379"))


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


async def read_feed_from_redis(stream_name: str) -> Optional[Feed]:
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
    # covers local execution
    if EXECUTE_LOCALLY:
        redis = await aioredis.from_url(
            LOCALHOST, port=REDISPORT, decode_responses=True
        )
    # execute in the aws cloud
    else:
        redis = await aioredis.from_url(
            READERHOST, port=REDISPORT, decode_responses=True
        )
    record_count = await redis.xlen(stream_name)
    if not record_count:
        return None
    print(f"reading from {stream_name} started")
    records = await redis.xrange(stream_name)
    print(f"total records loaded {len(records)}")

    return Feed(stream_name, records)


async def live_feed_ingestor(stream_name: str) -> None:
    """Pushes feed data from redis to posttgres
    and clears the redis memory of ids pushed

    Parameters
    ----------
    stream_name : str
        the name of the stream from exchange
    redis : Redis
        _description_
    """
    feed = await read_feed_from_redis(stream_name)
    if not feed:
        return
    table_name = stream_name.replace("-", "_")
    for batch in batch_records(feed.records, 10000):
        push_feed_to_questdb(table_name, stream_name, batch)
    # remove all ids in feed from stream
    redis = await aioredis.from_url(WRITERHOST, port=REDISPORT)
    print(f"deleting ids from {stream_name}")
    await redis.xdel(stream_name, *feed.get_ids())


if __name__ == "__main__":
    start = perf_counter()
    asyncio.run(live_feed_ingestor("binance-L1"))
    end = perf_counter()
    print(f"the program finished in {(end - start):.4f} seconds")
