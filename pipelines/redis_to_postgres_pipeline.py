import subprocess

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.tasks import Task

from pipelines.prefecttoolz import build_command


@task(
    name="execute shell command",
    description="runs a shelltask to read from redis and push to postgres and clean redis",
    retries=3,
    retry_delay_seconds=10,
)
def run_in_shell(command: Task):
    subprocess.run(command, shell=True)


@flow(name="redis_to_postgres_pipeline", task_runner=ConcurrentTaskRunner)
def run_flow(shell_task: str):
    cmd = build_command(shell_task)
    run_in_shell(command=cmd)
