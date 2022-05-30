import math
import socket
from typing import Any, Dict, List

from influx_line_protocol import Metric

AWSHOST: str = "18.207.241.164"
INFLUXPORT: int = 9009


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
