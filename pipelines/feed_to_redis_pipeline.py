import subprocess
from typing import List

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.tasks import Task

from pipelines.prefecttoolz import build_command

# shell task to run python programs


@task(
    name="execute shell command",
    description="runs each of the exchange.py file as a separate shell task",
    retries=3,
    retry_delay_seconds=10,
)
def run_streamer_in_shell(command: Task):
    subprocess.run(command, shell=True)


@flow(name="live_feeds_to_redis_pipeline", task_runner=ConcurrentTaskRunner)
def run_flow(shell_task: str, symbol: str, stream_name: str):
    cmd = build_command(shell_task, symbol=symbol, stream_name=stream_name)
    run_streamer_in_shell(command=cmd)
