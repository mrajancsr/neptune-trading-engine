import os
from os.path import join

from exchange_feeds.constants import EXCHANGEPATH, PHOBOSPATH
from prefect.deployments import DeploymentSpec

path_to_pipeline = join(PHOBOSPATH, "pipelines", "feed_to_redis_pipeline.py")
path_to_file = os.path.join(EXCHANGEPATH, "feed_ingestor_to_redis.py")


DeploymentSpec(
    name="kraken_L1_btcusd",
    flow_location=path_to_pipeline,
    tags=["kraken-L1", "btcusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "BTC/USD",
        "stream_name": "kraken-orderbook",
    },
)

DeploymentSpec(
    name="kraken_L1_ethusd",
    flow_location=path_to_pipeline,
    tags=["kraken-L1", "ethusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "ETH/USD",
        "stream_name": "kraken-orderbook",
    },
)

DeploymentSpec(
    name="kraken_L1_bnbusd",
    flow_location=path_to_pipeline,
    tags=["kraken-L1", "bnbusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "BNB/USD",
        "stream_name": "kraken-orderbook",
    },
)

DeploymentSpec(
    name="kraken_L1_xrpusd",
    flow_location=path_to_pipeline,
    tags=["kraken-L1", "xrpusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "XRP/USD",
        "stream_name": "kraken-orderbook",
    },
)

DeploymentSpec(
    name="kraken_L1_lunausd",
    flow_location=path_to_pipeline,
    tags=["Kraken-L1", "lunausd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "LUNA/USD",
        "stream_name": "kraken-orderbook",
    },
)

DeploymentSpec(
    name="kraken_L1_solusd",
    flow_location=path_to_pipeline,
    tags=["kraken-L1", "solusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "SOL/USD",
        "stream_name": "kraken-orderbook",
    },
)

DeploymentSpec(
    name="kraken_L1_avaxusd",
    flow_location=path_to_pipeline,
    tags=["kraken-L1", "avaxusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "AVAX/USD",
        "stream_name": "kraken-orderbook",
    },
)

DeploymentSpec(
    name="kraken_L1_dotusd",
    flow_location=path_to_pipeline,
    tags=["kraken-L1", "dotusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "DOT/USD",
        "stream_name": "kraken-orderbook",
    },
)

DeploymentSpec(
    name="kraken_L1_dogeusd",
    flow_location=path_to_pipeline,
    tags=["kraken-L1", "dogeusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "DOGE/USD",
        "stream_name": "kraken-orderbook",
    },
)

DeploymentSpec(
    name="kraken_L1_adausd",
    flow_location=path_to_pipeline,
    tags=["kraken-L1", "adausd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "ADA/USD",
        "stream_name": "kraken-orderbook",
    },
)
