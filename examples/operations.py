"""Operational settings: retries, pools, timeouts, callbacks, SLAs, sensors, cross-DAG deps.

Airflow original (spread across several DAGs):

    default_args = {"retries": 3, "retry_delay": timedelta(minutes=1),
                    "retry_exponential_backoff": True, "pool": "warehouse",
                    "execution_timeout": timedelta(hours=1),
                    "on_failure_callback": notify_slack, "sla": timedelta(hours=6)}
    FileSensor(task_id="wait_for_drop", filepath="/data/drop/ready.flag", poke_interval=60)
    ExternalTaskSensor(task_id="wait_upstream", external_dag_id="orders_etl")
"""

from datetime import timedelta
from pathlib import Path

import dagster as dg


def notify(message: str) -> None:
    """Stands in for notify_slack."""


@dg.asset(
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60, backoff=dg.Backoff.EXPONENTIAL),
    pool="warehouse",  # Airflow pool: set its limit with `dagster instance concurrency set`
    # sla=6h: fail if not updated within 6 hours (shown in the UI, alertable).
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=6)),
)
def warehouse_report() -> None: ...


# ExternalTaskSensor on a DAG that is ALSO migrated: depend on its asset directly,
# and let the asset run when the upstream updates instead of polling.
@dg.asset(
    deps=[dg.AssetKey("orders_table")],
    automation_condition=dg.AutomationCondition.eager(),
)
def orders_summary() -> None: ...


report_job = dg.define_asset_job(
    "report_job",
    selection=[warehouse_report],
    # execution_timeout: run-level limit, enforced only when run monitoring is enabled.
    tags={"dagster/max_runtime": 3600},
)


# on_failure_callback -> a run failure sensor (one place, covers every listed job).
@dg.run_failure_sensor(monitored_jobs=[report_job])
def notify_on_failure(context: dg.RunFailureSensorContext) -> None:
    notify(f"{context.dagster_run.job_name} failed: {context.failure_event.message}")


DROP_FLAG = Path("/data/drop/ready.flag")


# FileSensor (or ExternalTaskSensor on something still outside Dagster) -> a sensor.
# The cursor stops the same file from triggering twice; Airflow got that from the DAG run.
@dg.sensor(job=report_job, minimum_interval_seconds=60)
def drop_ready_sensor(context: dg.SensorEvaluationContext):
    if not DROP_FLAG.exists():
        return dg.SkipReason(f"{DROP_FLAG} not present")
    mtime = str(DROP_FLAG.stat().st_mtime)
    if mtime == context.cursor:
        return dg.SkipReason("already processed this drop")
    context.update_cursor(mtime)
    return dg.RunRequest(run_key=mtime)


defs = dg.Definitions(
    assets=[warehouse_report, orders_summary],
    jobs=[report_job],
    sensors=[notify_on_failure, drop_ready_sensor],
)
