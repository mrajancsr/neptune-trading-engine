from os.path import join
from typing import List

from exchange_feeds.constants import EXCHANGEPATH, PHOBOSPATH
from prefect.deployments import DeploymentSpec

path_to_pipeline = join(PHOBOSPATH, "pipelines", "feed_to_redis_pipeline.py")

path_to_ex_dir = join(EXCHANGEPATH, "dydx")
file_names = ["dydx_blotter.py", "dydx_orderbook.py"]
deployment_name = "dydx-feed-to-redis-deployment"

# contains path to above files
tasks: List[str] = list(map(lambda x: join(path_to_ex_dir, x), file_names))


DeploymentSpec(
    name=deployment_name,
    flow_location=path_to_pipeline,
    tags=["exchange-streamer", "raj"],
    parameters={"shell_tasks": tasks},
)
