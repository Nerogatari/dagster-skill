# Operators, hooks, connections and variables

Patterns follow the official operator guides:
https://dagster.io/docs/migration/airflow-to-dagster/airflow-operator-migration

## PythonOperator / @task

Call the same function from the asset. If Airflow and Dagster must both run it during the
cutover, move it into a shared package that both can import (an editable install in each
project).

```python
from shared_pipelines.orders import build_orders  # was python_callable=build_orders

@dg.asset
def orders_table() -> None:
    build_orders()
```

`op_kwargs` become arguments, or a `dg.Config` class when they changed per run.

## BashOperator

Use Pipes so the logs and the exit code show up in Dagster:

```python
@dg.asset
def script_result(context: dg.AssetExecutionContext, pipes: dg.PipesSubprocessClient):
    return pipes.run(context=context, command=["python", "/path/to/script.py"]).get_results()

defs = dg.Definitions(assets=[script_result], resources={"pipes": dg.PipesSubprocessClient()})
```

- Pass the command as a list, not a shell string, unless it needs shell features.
- Templated `bash_command` values (`{{ ds }}`) become arguments built from the partition (see
  `scheduling.md`), and `env=` becomes the `env=` of `run(...)`.
- A `BashOperator` that runs `dbt` → `dagster-dbt` (`@dbt_assets` over the dbt manifest),
  which produces one asset per dbt model instead of one opaque task.

## KubernetesPodOperator

```python
import dagster_k8s

@dg.asset
def k8s_task(context: dg.AssetExecutionContext):
    return (
        dagster_k8s.PipesK8sClient()
        .run(context=context,
             base_pod_meta={"name": "my-task"},
             base_pod_spec={"containers": [{"name": "main", "image": "...", "command": [...]}]})
        .get_results()
    )
```

## SQL operators (PostgresOperator, SQLExecuteQueryOperator, Snowflake, BigQuery)

Define a resource that wraps the client library. Run the same SQL in the asset, scoped to
the partition when the SQL was templated.

```python
class Postgres(dg.ConfigurableResource):
    dsn: str

    def execute(self, sql: str, params: dict | None = None) -> None:
        import psycopg
        with psycopg.connect(self.dsn) as conn:
            conn.execute(sql, params)

@dg.asset(partitions_def=daily)
def customers_table(context: dg.AssetExecutionContext, postgres: Postgres) -> None:
    postgres.execute("DELETE FROM customers WHERE day = %(ds)s", {"ds": context.partition_key})
    postgres.execute(Path("sql/customers.sql").read_text(), {"ds": context.partition_key})

defs = dg.Definitions(assets=[customers_table],
                      resources={"postgres": Postgres(dsn=dg.EnvVar("CUSTOMERS_DSN"))})
```

Check for an existing integration before writing your own resource: `dagster-snowflake`,
`dagster-gcp` (BigQuery), `dagster-duckdb`, `dagster-aws`, `dagster-databricks`.

## Hooks and Connections

- A Connection (`conn_id`) → a `dg.ConfigurableResource` with one field per connection
  attribute, filled from `dg.EnvVar`. Name the resource key after the old `conn_id` so
  reviewers can trace it.
- Hook methods the tasks used → methods on the resource (see `examples/basic_etl.py`).
- Never keep `BaseHook.get_connection`, or a connection string literal, in the Dagster code.

## Variables

- `Variable.get("x")` inside a task → a resource field or a `dg.Config` field.
- `Variable.get` at module top level ran on every DAG parse. It must not move to module
  level in Dagster either: code locations import the module on every load. Read it inside a
  resource.

## Sensors and waiting operators

- `FileSensor`, `S3KeySensor`, `HttpSensor`, `SqlSensor` → `@dg.sensor` that checks once per
  tick and keeps a **cursor** (latest mtime, key or row id) so the same event doesn't trigger
  twice. `poke_interval` → `minimum_interval_seconds`. `timeout` and `mode="reschedule"` have
  no equivalent: sensors don't hold a worker slot.
- `ExternalTaskSensor` → see `control-flow.md`.

## Notification operators

`EmailOperator`, `SlackWebhookOperator` and similar operators at the end of a DAG → a
`@dg.run_status_sensor` (on success) or a `@dg.run_failure_sensor`, or a call inside the last
asset when the message contains the asset's results. These are not assets.
