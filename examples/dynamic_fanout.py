"""Dynamic task mapping (.expand) -> dynamic partitions, or an in-run fan-out.

Airflow original:

    @task
    def list_files() -> list[str]: ...
    @task
    def process(path: str): ...
    process.expand(path=list_files())

Pick by what one mapped item is:
- A durable unit you want to retry, backfill or inspect alone (a file, a tenant)
  -> DynamicPartitionsDefinition + a sensor that adds partitions.
- Just parallel work inside one run -> graph_asset with DynamicOut, then collect.
- Few, cheap items -> a loop inside one asset.
"""

import dagster as dg

# --- Option 1: each file is its own partition ------------------------------------

files = dg.DynamicPartitionsDefinition(name="files")


@dg.asset(partitions_def=files)
def processed_file(context: dg.AssetExecutionContext) -> None:
    context.log.info(f"processing {context.partition_key}")


processed_file_job = dg.define_asset_job("processed_file_job", selection=[processed_file])


def list_files() -> list[str]:
    """Stands in for the old list_files task (an S3 listing, a directory scan, ...)."""
    return ["a.csv", "b.csv"]


@dg.sensor(job=processed_file_job, minimum_interval_seconds=60)
def new_files_sensor(context: dg.SensorEvaluationContext):
    new = [f for f in list_files() if not context.instance.has_dynamic_partition(files.name, f)]
    return dg.SensorResult(
        run_requests=[dg.RunRequest(partition_key=f) for f in new],
        dynamic_partitions_requests=[files.build_add_request(new)],
    )


# --- Option 2: parallel fan-out inside one run --------------------------------------


@dg.op(out=dg.DynamicOut(str))
def fan_out_files():
    for path in list_files():
        yield dg.DynamicOutput(path, mapping_key=path.replace(".", "_"))


@dg.op
def process_one(path: str) -> int:
    return len(path)


@dg.op
def combine(sizes: list[int]) -> int:
    return sum(sizes)


@dg.graph_asset
def files_summary() -> int:
    return combine(fan_out_files().map(process_one).collect())


defs = dg.Definitions(
    assets=[processed_file, files_summary],
    jobs=[processed_file_job],
    sensors=[new_files_sensor],
)
