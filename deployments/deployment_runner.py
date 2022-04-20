import os
import subprocess
from time import sleep
from typing import List

from exchange_feeds.constants import ACCOUNT, FLOW_NAME, WORKSPACE_NAME

ID = os.path.join(ACCOUNT, WORKSPACE_NAME)


def cloud_login():
    key = os.environ["PREFECTCLOUDKEY"]
    cmd = f"prefect cloud login --key {key} -w {ID}"
    subprocess.run(cmd, shell=True)


def create_deployments(deployment_name):
    cmd = f"prefect deployment create {deployment_name}"
    subprocess.run(cmd, shell=True)


def deployment_ls():
    cmd = "prefect deployment ls"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(result.stdout)


def run_unscheduled_deployments(deployment_names: List[str]):
    for d in deployment_names:
        cmd = f"prefect deployment run {d}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print(result.stdout)


def start_prefect_agent(agent_name):
    cmd = f"prefect work-queue create {agent_name}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    uiud = result.stdout.strip("UIUD()").replace("(", "").replace(")", "")
    cmd = f"prefect agent start {uiud}"
    subprocess.run(cmd, shell=True)


def sign_into_cloud_and_create_deployments():
    print("logging into prefect cloud")
    cloud_login()
    print("Successfully logged into prefect cloud")
    print("Creating redis to postgres deployment")
    # create_deployments("redis_to_postgres_deployment.py")
    sleep(5)
    print("Creating FTX deployments")
    # create_deployments("ftx/orderbook_deployment.py")
    # sleep(5)
    # create_deployments("ftx/blotter_deployment.py")
    # sleep(5)
    # create_deployments("kraken/orderbook_deployment.py")
    # sleep(5)
    # create_deployments("kraken/blotter_deployment.py")
    # sleep(5)


if __name__ == "__main__":
    sign_into_cloud_and_create_deployments()
    deployment_ls()
