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
from neptunedb import DBReader

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
class RawFeed:
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


def handle_record(
    record: Dict,
    stream_name: str,
    handle_lob: bool,
    symbol: Optional[str] = None,
    book: LimitOrderBook = None,
) -> Dict[str, Any]:
    """Pushes a single record to redis
    Parameters
    ----------
    record : Dict
        message record from exchange
    stream_name : str
        name of the stream in redis
    pipe : Pipeline
        a redis pipeline that minimizes round trip trxns
    handle_lob : bool
        whether to process the lob or not
        returns the best prices at the moment
    symbol : Optional[str], optional, default=None
        the ticker of interest
    """
    if handle_lob and symbol is None:
        raise KeyError("symbol must be provided in handling lob")
    elif handle_lob and symbol is not None:
        book.update_bids(record)
        book.update_asks(record)
        if not book.ask_size() or not book.bid_size():
            return

        bid = book.best_bid()
        ask = book.best_ask()
        new_record = book.to_frame() if stream_name in L2_ENABLED_STREAMS else {}
        new_record["symbol"] = symbol
        new_record["best_bid"] = bid.price
        new_record["bid_size"] = bid.size
        new_record["best_ask"] = ask.price
        new_record["ask_size"] = ask.size
        new_record["trxn_time"] = new_record.get(
            "trxn_time", max(bid.offset, ask.offset)
        )

        if stream_name in L2_ENABLED_STREAMS:
            new_record["best_bid_timestamp"] = bid.offset
            new_record["best_ask_timestamp"] = ask.offset

        return new_record
    else:
        return record


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


async def push_raw_feeds_to_redis(
    obj: Union[Blotter, Book],
    stream_name: str,
    pipe: Pipeline,
    handle_lob: bool = False,
    save_stream: bool = False,
    max_record_count: int = 500,
) -> None:
    """Pushes raw feeds received from exchange to redis
    Parameters
    ----------
    obj : Exchange
        one of blotter or order book
    stream_name : str
        name of the stream in redis
    redis : Redis
        the database to push feeds into
    pipe : Pipeline
        a redis pipeline that minimizes round trip transactions
    """
    print("Socket open")
    record_count = 0

    book = LimitOrderBook()

    symbol = obj.symbol.replace("-", "").replace("/", "")
    await obj.send()

    while True:
        if not obj.websocket.open:
            try:
                print("Websocket is NOT connected.  ")
                obj.websocket = await websockets.connect(obj.uri)
                await obj.send()
            except Exception:
                print("Unable to reconnect, trying again...")
        try:
            records = await obj.receive()
            if not records:
                continue

            record_count += len(records)
            for record in records:
                record = handle_record(record, stream_name, handle_lob, symbol, book)
                print(record)
                if save_stream:
                    await pipe.xadd(stream_name, record)
            if not save_stream:
                continue
            if record_count >= max_record_count:
                print("entered here")
                await pipe.execute()
                # -some exchanges require a pong when they send a ping frame
                if obj.exchange == "Ftx":
                    await obj.websocket.ping()
                else:
                    await obj.websocket.pong()
                print("pushed to redis")
                record_count = 0
                continue
        except Exception as e:
            msg = f"""Socket Closed: at {datetime.now()};
            close reason: {obj.websocket.close_reason},
                close code: {obj.websocket.close_code}"""
            obj.logger.info(msg)
            obj.logger.debug(e)
            obj.logger.info(f"error occured in main body {stream_name}-{symbol}")
            continue


async def read_feed_from_redis_once(
    stream_name: str, redis: Optional[Redis] = None
) -> Optional[RawFeed]:
    """Reads a single feed from redis
    Parameters
    ----------
    stream_name : str
        the exchange stream name
    redis : Optional[Redis], default=None
        connection from localhost,
    Returns
    -------
    Optional[RawFeed]
        contains stream name and raw feed
    """
    if not redis:
        redis = await connect_to_redis(action=RedisActionType.READ_ONLY)
    record_count = await redis.xlen(stream_name)
    if not record_count:
        return
    print(f"reading from {stream_name} started")
    feed = RawFeed(stream_name, records=await redis.xrange(stream_name))

    return feed


async def read_live_feeds_from_redis(
    stream_name: str, redis: Optional[Redis] = None
) -> Optional[RawFeed]:
    """_summary_
    Parameters
    ----------
    stream_name : str
        _description_
    redis : Optional[Redis], optional
        _description_, by default None
    Returns
    -------
    Optional[RawFeed]
        _description_
    """
    if not redis:
        redis = await connect_to_redis(action=RedisActionType.READ_ONLY)

    while True:
        record = await redis.xread({stream_name: b"$"}, block=0)


async def push_feed_to_postgres(feed: RawFeed) -> None:
    db = DBReader()
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


def recursivels(phobos_path: str, files: list, exclusions: Set[str]) -> None:
    """Recursively get all files names in phobos path
    Parameters
    ----------
    phobos_path : str
        parent directory where folders are located
    files : list
        stores the files names in list
    """
    if os.path.isfile(phobos_path) and phobos_path not in exclusions:
        files.append(phobos_path)
    elif os.path.isdir(phobos_path) and not phobos_path.startswith("."):
        for entry in os.listdir(phobos_path):
            if (
                entry.startswith("__")
                or entry.startswith(".")
                or entry.endswith(".ipynb")
                or entry in exclusions
            ):
                continue
            subdir = os.path.join(phobos_path, entry)
            recursivels(subdir, files, exclusions)
