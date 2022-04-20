from prefect import task
from prefect.tasks import Task


@task
def build_command(path_to_file: str, **kwargs) -> Task:
    if "symbol" in kwargs and "stream_name" in kwargs:
        symbol = kwargs["symbol"]
        stream_name = kwargs["stream_name"]
        return f"""python3 {path_to_file} -t '{symbol}' -n '{stream_name}' -s"""
    return f"python3 {path_to_file}"
