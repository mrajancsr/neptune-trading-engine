"""The purpose of this file is to clean the redis data streams in memory every
15 minutes and then push the cleaned data to a historical database
"""
import asyncio
from time import perf_counter

import uvloop
from redistoolz import connect_to_redis, push_feed_to_postgres, read_feed_from_redis

from constants import STREAM_NAMES
from exchange_feeds.constants import RedisActionType

# event policy needs to be set at top of file
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def push_feed_from_redis_to_postgres(
    stream_name: str, redis_reader, redis_writer
) -> None:
    """Pushes feed data from redis to posttgres
    and clears the redis memory of ids pushed

    Parameters
    ----------
    stream_name : str
        the name of the stream from exchange
    redis : Redis
        a redis connection
    """
    feed = await read_feed_from_redis(stream_name, redis_reader)
    if not feed:
        return

    await push_feed_to_postgres(feed)
    # remove all ids in feed from stream
    print(f"deleting ids from {stream_name}")

    await redis_writer.xdel(stream_name, *feed.get_ids())


async def live_feed_ingestor() -> None:
    reader = await connect_to_redis()
    writer = await connect_to_redis(RedisActionType.WRITE_ONLY)
    tasks = [
        asyncio.create_task(push_feed_from_redis_to_postgres(stream, reader, writer))
        for stream in STREAM_NAMES
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    start = perf_counter()
    asyncio.run(live_feed_ingestor())
    end = perf_counter()
    print(f"the program finished in {(end - start):.4f} seconds")
