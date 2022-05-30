"""The purpose of this file is to clean the redis data streams in memory every
15 minutes and then push the cleaned data to a historical database
"""
import asyncio
from time import perf_counter

import uvloop
from aioredis import Redis
from redistoolz import (
    batch_records,
    connect_to_redis,
    push_feed_to_questdb,
    read_feed_from_redis,
)

# event policy needs to be set at top of file
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def push_feed_from_redis_to_questdb(
    stream_name: str,
    redis: Redis,
) -> None:
    """Pushes feed data from redis to posttgres
    and clears the redis memory of ids pushed

    Parameters
    ----------
    stream_name : str
        the name of the stream from exchange
    redis : Redis
        _description_
    """
    feed = await read_feed_from_redis(stream_name, redis)
    if not feed:
        return
    table_name = stream_name.replace("-", "_")
    for batch in batch_records(feed.records, 10000):
        push_feed_to_questdb(table_name, stream_name, batch)
    # remove all ids in feed from stream
    print(f"deleting ids from {stream_name}")
    await redis.xdel(stream_name, *feed.get_ids())


async def live_feed_ingestor() -> None:
    redis = await connect_to_redis()
    tasks = [
        asyncio.create_task(push_feed_from_redis_to_questdb(stream, redis))
        for stream in ["binance-L1"]
    ]
    await asyncio.gather(*tasks)
    await redis.close()


if __name__ == "__main__":
    start = perf_counter()
    asyncio.run(live_feed_ingestor())
    end = perf_counter()
    print(f"the program finished in {(end - start):.4f} seconds")
