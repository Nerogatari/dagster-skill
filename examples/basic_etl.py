"""Basic TaskFlow DAG -> assets, with a Connection -> Resource and an asset check.

Airflow original:

    @dag(schedule="0 9 * * *", start_date=datetime(2024, 1, 1), catchup=False,
         tags=["orders"], default_args={"retries": 2, "retry_delay": timedelta(minutes=5)})
    def orders_etl():
        @task
        def extract() -> list[dict]:
            return fetch_orders()                       # small payload, passed by XCom

        @task
        def load(rows: list[dict]):
            hook = SqliteHook(sqlite_conn_id="warehouse")  # Airflow Connection
            hook.insert_rows("orders", rows, replace=True)

        load(extract())
"""

import sqlite3

import dagster as dg


def fetch_orders() -> list[dict]:
    """Stands in for the business logic the Airflow task called; keep it unchanged."""
    return [{"id": 1, "amount": 10.0}, {"id": 2, "amount": 25.5}]


class Warehouse(dg.ConfigurableResource):
    """Replaces the Airflow Connection `warehouse`. Configured from env, never hard-coded."""

    path: str

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)


RETRIES = dg.RetryPolicy(max_retries=2, delay=300)  # default_args retries / retry_delay


@dg.asset(group_name="orders_etl", kinds={"python"}, retry_policy=RETRIES)
def raw_orders() -> list[dict]:
    # Small payload, so the I/O manager carries it (closest to the old XCom).
    return fetch_orders()


@dg.asset(group_name="orders_etl", kinds={"sqlite"}, retry_policy=RETRIES)
def orders_table(raw_orders: list[dict], warehouse: Warehouse) -> dg.MaterializeResult:
    # The table lives in the warehouse, so nothing is returned for the I/O manager.
    with warehouse.connect() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, amount REAL)")
        conn.executemany("INSERT OR REPLACE INTO orders VALUES (:id, :amount)", raw_orders)
    return dg.MaterializeResult(metadata={"row_count": len(raw_orders)})


@dg.asset_check(asset=orders_table)
def orders_not_empty(warehouse: Warehouse) -> dg.AssetCheckResult:
    # Query the real table; orders_table returns nothing, so there is no value to load.
    with warehouse.connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    return dg.AssetCheckResult(passed=count > 0, metadata={"row_count": count})


orders_job = dg.define_asset_job("orders_etl_job", selection=dg.AssetSelection.groups("orders_etl"))

orders_schedule = dg.ScheduleDefinition(
    job=orders_job,
    cron_schedule="0 9 * * *",
    execution_timezone="UTC",  # Airflow schedules run in the DAG's timezone (UTC unless set)
)

defs = dg.Definitions(
    assets=[raw_orders, orders_table],
    asset_checks=[orders_not_empty],
    jobs=[orders_job],
    schedules=[orders_schedule],
    resources={"warehouse": Warehouse(path=dg.EnvVar("WAREHOUSE_PATH"))},
)
