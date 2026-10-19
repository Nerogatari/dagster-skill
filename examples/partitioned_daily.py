"""Interval DAG using {{ ds }} / data_interval_* -> time-partitioned asset.

Airflow original:

    @dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=True)
    def daily_sales():
        @task
        def build(ds=None, data_interval_start=None, data_interval_end=None):
            run_sql(f"DELETE FROM daily_sales WHERE day = '{ds}'")
            run_sql(f"INSERT INTO daily_sales SELECT ... WHERE ts >= '{data_interval_start}'"
                    f" AND ts < '{data_interval_end}'")

The Airflow run for ds=2024-01-01 starts at 2024-01-02 00:00 (end of its interval).
build_schedule_from_partitioned_job keeps that: at 2024-01-02 00:00 it requests
partition "2024-01-01".
"""

import sqlite3

import dagster as dg

daily = dg.DailyPartitionsDefinition(start_date="2024-01-01", timezone="UTC")


class Warehouse(dg.ConfigurableResource):
    path: str

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)


@dg.asset(partitions_def=daily, group_name="sales", kinds={"sqlite"})
def daily_sales(context: dg.AssetExecutionContext, warehouse: Warehouse) -> dg.MaterializeResult:
    day = context.partition_key  # Airflow {{ ds }}
    window = context.partition_time_window  # Airflow data_interval_start / data_interval_end
    with warehouse.connect() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS daily_sales (day TEXT, window_start TEXT, window_end TEXT)")
        # Delete-then-insert keeps reruns and backfills idempotent, as the DAG did.
        conn.execute("DELETE FROM daily_sales WHERE day = ?", (day,))
        conn.execute(
            "INSERT INTO daily_sales VALUES (?, ?, ?)",
            (day, window.start.isoformat(), window.end.isoformat()),
        )
    return dg.MaterializeResult(metadata={"day": day})


@dg.asset(
    partitions_def=daily,
    group_name="sales",
    # depends_on_past=True: each day also depends on the previous day of itself, so
    # backfills and automation conditions build days in order. Schedules do not wait on it.
    deps=[
        daily_sales,
        dg.AssetDep(
            "cumulative_sales",
            partition_mapping=dg.TimeWindowPartitionMapping(start_offset=-1, end_offset=-1),
        ),
    ],
)
def cumulative_sales(context: dg.AssetExecutionContext) -> None:
    context.log.info(f"cumulating through {context.partition_key}")


daily_sales_job = dg.define_asset_job("daily_sales_job", selection=[daily_sales, cumulative_sales])

# catchup=True has no equivalent: the schedule only requests the latest partition.
# Fill history with a backfill (UI or `dagster job backfill`), after checking idempotency.
daily_sales_schedule = dg.build_schedule_from_partitioned_job(daily_sales_job)

defs = dg.Definitions(
    assets=[daily_sales, cumulative_sales],
    jobs=[daily_sales_job],
    schedules=[daily_sales_schedule],
    resources={"warehouse": Warehouse(path=dg.EnvVar("WAREHOUSE_PATH"))},
)
