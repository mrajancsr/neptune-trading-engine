import os
from datetime import timedelta

from exchange_feeds.constants import EXCHANGEPATH, PHOBOSPATH
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import IntervalSchedule

path_to_file = os.path.join(EXCHANGEPATH, "feed_ingestor_to_postgres.py")
path_to_pipeline = os.path.join(
    PHOBOSPATH, "pipelines", "redis_to_postgres_pipeline.py"
)
DEPLOYMENT_NAME = "redis-to-postgres-deployment"

schedule = IntervalSchedule(interval=timedelta(minutes=5))

DeploymentSpec(
    name=DEPLOYMENT_NAME,
    flow_location=path_to_pipeline,
    tags=["exchange-hist", "phobos"],
    parameters={"shell_task": path_to_file},
    schedule=schedule,
)
