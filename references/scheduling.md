# Scheduling, intervals and run ordering

Tested in `examples/partitioned_daily.py` and `tests/test_examples.py`.

## First question: does the DAG use its interval?

Search the DAG for `ds`, `ts`, `data_interval_start`, `data_interval_end`, `logical_date`,
`execution_date`, `prev_ds`, `next_ds`, `macros.`, and `{{` in templated fields (`sql=`,
`bash_command=`, `params=`, file paths).

- **No interval use** (the task works on "whatever is there now"):
  `dg.ScheduleDefinition(job=..., cron_schedule=..., execution_timezone=...)`.
- **Interval use**: a partitioned asset plus `dg.build_schedule_from_partitioned_job(job)`.
  Don't port an interval DAG to a plain cron schedule that computes "yesterday" by hand. It
  breaks reruns, and backfills become impossible.

`schedule=None` → no schedule; the job runs manually or from a sensor.

## Choosing the partitions definition

| Airflow schedule | Partitions definition |
|---|---|
| `@daily` / `0 0 * * *` | `dg.DailyPartitionsDefinition(start_date=..., timezone=...)` |
| `0 9 * * *` (daily, not at midnight) | `dg.DailyPartitionsDefinition(start_date=..., hour_offset=9)`: window 09:00 → 09:00, scheduled at 09:00 |
| `@hourly` | `dg.HourlyPartitionsDefinition(start_date="2024-01-01-00:00")` |
| `@weekly` (`0 0 * * 0`) | `dg.WeeklyPartitionsDefinition(start_date=...)` |
| `@monthly` (`0 0 1 * *`) | `dg.MonthlyPartitionsDefinition(start_date=...)` |
| Other cron | `dg.TimeWindowPartitionsDefinition(cron_schedule=..., start=..., fmt=...)` |

- `start_date` = the DAG's `start_date`.
- `timezone` = the timezone of the DAG's `start_date` (pendulum `tz`), or Airflow's
  `default_timezone` setting, which is UTC unless changed.
- Use explicit cron strings rather than `@daily`-style presets in Dagster code.

**Timing parity (verified):** an Airflow run for `ds=2024-01-01` starts at the end of its
interval (2024-01-02 00:00). `build_schedule_from_partitioned_job` does the same: the tick at
2024-01-02 00:00 requests partition `2024-01-01`. The same holds with `hour_offset=9`.

## Template variables → context

Inside an asset with `context: dg.AssetExecutionContext`:

| Airflow | Dagster |
|---|---|
| `{{ ds }}` (daily) | `context.partition_key` (`"2024-01-01"`) |
| `{{ ds_nodash }}` | `context.partition_key.replace("-", "")` |
| `{{ data_interval_start }}` / `{{ logical_date }}` / `{{ execution_date }}` | `context.partition_time_window.start` |
| `{{ data_interval_end }}` | `context.partition_time_window.end` |
| `{{ ts }}` | `context.partition_time_window.start.isoformat()` |
| `{{ prev_ds }}`, `{{ macros.ds_add(ds, -1) }}` | `(context.partition_time_window.start - timedelta(days=1)).strftime("%Y-%m-%d")` |
| `{{ next_ds }}` | `context.partition_time_window.end.strftime("%Y-%m-%d")` |
| `{{ run_id }}` | `context.run_id` |
| `{{ params.x }}`, `{{ dag_run.conf["x"] }}` | A `dg.Config` subclass as an asset parameter (run config) |
| `{{ var.value.x }}` | A resource field fed by `dg.EnvVar` |

Hourly partition keys look like `"2024-01-01-03:00"`, which is **not** the same string as
Airflow's `ts`. Derive strings from `partition_time_window` instead of reusing the key.

For templated SQL files, render them with the values above, for example
`sql.format(ds=context.partition_key)` or Jinja with the same variable names, so the `.sql`
files stay unchanged.

## catchup and backfills

- `catchup=True`: Dagster never backfills on deploy. The schedule requests only the newest
  partition. Tell the user to run a backfill over the range, from the UI or with
  `dagster job backfill -j <job> --from <start> --to <end>`, and only after checking that the
  writes are idempotent.
- `catchup=False`: nothing to do. Start the schedule and history stays empty.
- Idempotency: write with delete-then-insert or `MERGE` scoped to the partition's window. A
  backfill or a retry then replaces the data instead of duplicating it.

## depends_on_past and wait_for_downstream

`depends_on_past=True` means the run for day N waits until the run for day N-1 succeeded.
The closest equivalent is a self-dependency on the previous partition:

```python
deps=[dg.AssetDep("cumulative_sales",
                  partition_mapping=dg.TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))]
```

Asset backfills and automation conditions respect this ordering. A scheduled run does
**not** wait on it. If a strict guard is needed, fail fast at the top of the asset:

```python
prev = (context.partition_time_window.start - timedelta(days=1)).strftime("%Y-%m-%d")
if prev >= START and prev not in context.instance.get_materialized_partitions(context.asset_key):
    raise dg.Failure(f"previous partition {prev} not materialized")
```

`wait_for_downstream` has no equivalent. Flag it as a behavior change.

## max_active_runs

Tag the job and limit runs per tag value in `dagster.yaml`
(on Dagster+, set this in the deployment settings):

```python
job = dg.define_asset_job("orders_etl_job", selection=..., tags={"airflow_dag": "orders_etl"})
```

```yaml
concurrency:
  runs:
    tag_concurrency_limits:
      - key: "airflow_dag"
        value: "orders_etl"
        limit: 1   # max_active_runs=1
```

`max_active_tasks` maps to pools; see `operations.md`.
