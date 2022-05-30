import math
import os
import socket
from dataclasses import dataclass, field
from json import loads
from typing import Any, Dict, Iterator, List, Optional, Set, TypeVar

import aioredis
from aioredis import Redis
from influx_line_protocol import Metric

from exchange_feeds.constants import L2_ENABLED_STREAMS, STREAM_NAMES, RedisActionType

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
AWSHOST: str = "18.207.241.164"
INFLUXPORT: int = 9009


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
    print(f"total records loaded {len(records)}")

    return Feed(stream_name, records)


def metric_type_setter(
    table_name: str, stream_name: str, records: List[Dict[str, Any]]
) -> str:
    metric = Metric(table_name)
    str_metric = ""
    metrics = ""
    if stream_name == "binance-L1":
        for record in records:
            metric.add_tag("symbol", record[1]["symbol"])
            metric.add_value("bid", float(record[1]["bid"]))
            metric.add_value("bid_size", float(record[1]["bid_size"]))
            metric.add_value("ask", float(record[1]["ask"]))
            metric.add_value("ask_size", float(record[1]["ask_size"]))
            metric.add_value("trxn_time", int(record[1]["trxn_time"]))
            str_metric = str(metric)
            str_metric += "\n"
            metrics += str_metric
    elif stream_name == "binance-blotter":
        for record in records:
            metric.add_value("E", int(record[1]["E"]))
            metric.add_value("a", int(record[1]["a"]))
            metric.add_tag("symbol", record[1]["s"])
            metric.add_value("p", float(record[1]["p"]))
            metric.add_value("q", float(record[1]["q"]))
            metric.add_value("f", int(record[1]["f"]))
            metric.add_value("l", int(record[1]["l"]))
            metric.add_value("T", int(record[1]["T"]))
            str_metric = str(metric)
            str_metric += "\n"
            metrics += str_metric
    return metrics


def batch_records(records: List[Any], batch_size: int) -> List[List[Any]]:
    num_batches: int = math.ceil(len(records) / batch_size)

    result = []
    for i in range(num_batches):
        result.append(records[i * batch_size : (i + 1) * batch_size])
    yield from result


def push_feed_to_questdb(table_name: str, stream_name: str, records) -> None:
    metrics: str = metric_type_setter(table_name, stream_name, records)
    bytes_metric = bytes(metrics, "utf-8")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((AWSHOST, INFLUXPORT))
    s.sendall(bytes_metric)
    s.close()


def recursivels(
    parent_path: str,
    files: List[str],
    exclusions: Set[str],
) -> None:
    """Recursively get all files names in parent path
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
