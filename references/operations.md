# Retries, concurrency, timeouts, alerts and SLAs

Tested in `examples/operations.py`.

| Airflow (`default_args` or task) | Dagster |
|---|---|
| `retries=3`, `retry_delay=timedelta(minutes=1)` | `retry_policy=dg.RetryPolicy(max_retries=3, delay=60)` on each asset; define it once as a constant |
| `retry_exponential_backoff=True` | `backoff=dg.Backoff.EXPONENTIAL` |
| `max_retry_delay` | No equivalent; note it |
| `pool="warehouse"` | `pool="warehouse"` on the asset, plus `dagster instance concurrency set warehouse <slots>`. Pool limits apply only after you set them. |
| `max_active_tasks` / `concurrency` | Pool limits, or `executor` `max_concurrent` in run config |
| `max_active_runs` | Run tag concurrency limits (see `scheduling.md`) |
| `execution_timeout` | Job tag `{"dagster/max_runtime": seconds}`. It is enforced only when run monitoring is enabled, and it applies to the whole run, not one step. Put an in-code timeout inside the asset when one step needs its own. |
| `on_failure_callback`, `email_on_failure` | `@dg.run_failure_sensor(monitored_jobs=[...])`, or `dagster-slack` / `dagster-msteams` sensor helpers |
| `on_success_callback` | `@dg.run_status_sensor(run_status=dg.DagsterRunStatus.SUCCESS, ...)` |
| `on_retry_callback` | No equivalent; note it |
| `sla=timedelta(hours=6)` | `freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=6))`, plus an alert on freshness failures |
| `owner` | `owners=["team:data-eng"]` or `owners=["person@example.com"]` on the asset |
| `doc_md` | `description=` |
| `priority_weight` | `op_tags={"dagster/priority": "<n>"}` on the asset (step priority within a run) |

## Retries need idempotent writes

`RetryPolicy` reruns the whole asset function. If the function appends rows, a retry
duplicates them. Make the write delete-then-insert or `MERGE`, scoped to the partition,
before carrying over `retries`.

## Data quality

Airflow DAGs often check data implicitly, for example a downstream task that fails on empty
input. Make those checks explicit with `@dg.asset_check`. Query the real storage inside the
check, because an asset that writes to a database returns nothing to load:

```python
@dg.asset_check(asset=orders_table)
def orders_not_empty(warehouse: Warehouse) -> dg.AssetCheckResult:
    with warehouse.connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    return dg.AssetCheckResult(passed=count > 0, metadata={"row_count": count})
```

Suggest at least one check per critical output: row count, null or uniqueness on keys, and
freshness.
