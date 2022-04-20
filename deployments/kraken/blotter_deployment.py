import os
from os.path import join

from exchange_feeds.constants import EXCHANGEPATH, PHOBOSPATH
from prefect.deployments import DeploymentSpec

path_to_pipeline = join(PHOBOSPATH, "pipelines", "feed_to_redis_pipeline.py")
path_to_file = os.path.join(EXCHANGEPATH, "feed_ingestor_to_redis.py")


DeploymentSpec(
    name="kraken_blotter_btcusd",
    flow_location=path_to_pipeline,
    tags=["kraken-blotter", "btcusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "BTC/USD",
        "stream_name": "kraken-blotter",
    },
)

DeploymentSpec(
    name="kraken_blotter_ethusd",
    flow_location=path_to_pipeline,
    tags=["kraken-blotter", "ethusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "ETH/USD",
        "stream_name": "kraken-blotter",
    },
)

DeploymentSpec(
    name="kraken_blotter_bnbusd",
    flow_location=path_to_pipeline,
    tags=["kraken_blotter", "bnbusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "BNB/USD",
        "stream_name": "kraken-blotter",
    },
)

DeploymentSpec(
    name="kraken_blotter_xrpusd",
    flow_location=path_to_pipeline,
    tags=["kraken-blotter", "xrpusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "XRP/USD",
        "stream_name": "kraken-blotter",
    },
)

DeploymentSpec(
    name="kraken_blotter_lunausd",
    flow_location=path_to_pipeline,
    tags=["kraken-blotter", "lunausd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "LUNA/USD",
        "stream_name": "kraken-blotter",
    },
)

DeploymentSpec(
    name="kraken_blotter_solusd",
    flow_location=path_to_pipeline,
    tags=["kraken-blotter", "solusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "SOL/USD",
        "stream_name": "kraken-blotter",
    },
)

DeploymentSpec(
    name="kraken_blotter_avaxusd",
    flow_location=path_to_pipeline,
    tags=["kraken-blotter", "avaxusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "AVAX/USD",
        "stream_name": "kraken-blotter",
    },
)

DeploymentSpec(
    name="kraken_blotter_dotusd",
    flow_location=path_to_pipeline,
    tags=["kraken-blotter", "dotusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "DOT/USD",
        "stream_name": "kraken-blotter",
    },
)

DeploymentSpec(
    name="kraken_blotter_dogeusd",
    flow_location=path_to_pipeline,
    tags=["kraken-blotter", "dogeusd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "DOGE/USD",
        "stream_name": "kraken-blotter",
    },
)

DeploymentSpec(
    name="kraken_blotter_adausd",
    flow_location=path_to_pipeline,
    tags=["kraken-blotter", "adausd"],
    parameters={
        "shell_task": path_to_file,
        "symbol": "ADA/USD",
        "stream_name": "kraken-blotter",
    },
)
