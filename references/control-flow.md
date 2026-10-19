# Branching, trigger rules, dynamic mapping and cross-DAG links

Tested in `examples/branching.py`, `examples/dynamic_fanout.py` and `examples/operations.py`.

## Branching

| Airflow | Dagster |
|---|---|
| Branch picks *how* to build the same data (full refresh vs incremental) | One asset with an `if`. Record the mode in `MaterializeResult(metadata=...)` |
| Branch picks *which* data to build | `@dg.multi_asset` with `dg.AssetOut(is_required=False)` per branch, `can_subset=False`; yield only the chosen `dg.Output` |
| `ShortCircuitOperator` / `@task.short_circuit` | `@dg.asset(output_required=False)` generator that yields nothing to stop |

Assets downstream of an output that was not produced are **skipped** in that run (verified).

### The join after a branch

Airflow joins use `trigger_rule="none_failed_min_one_success"` so the join runs whichever
branch was taken. In Dagster, an asset that depends on **both** optional outputs is
**skipped**, because one of them never ran (verified in `test_join_on_both_branches_is_skipped`).

Fix: have the branching multi_asset also yield an output that is always produced (for
example a `load_log`), and make the join depend on that one. If the branch is really
"same data, two methods", collapse it into one asset instead, and the problem disappears.

## Trigger rules

| `trigger_rule` | Dagster |
|---|---|
| `all_success` (default) | Default behavior |
| `none_failed_min_one_success`, `none_failed` (join after branch) | See *The join after a branch* |
| `all_done`, `always` (cleanup that runs whatever happens) | `try/finally` inside the asset that creates the thing to clean up. For run-level cleanup, a `@dg.run_status_sensor` / `@dg.run_failure_sensor` |
| `one_failed`, `all_failed` (alert or compensate on failure) | `@dg.run_failure_sensor(monitored_jobs=[...])` |
| `one_success` | No equivalent; restructure, and flag it as a behavior change |

## Dynamic task mapping (`.expand`)

Ask what one mapped item is.

| Item | Dagster | Why |
|---|---|---|
| A durable unit (file, tenant, date) you want to retry, backfill or inspect alone | `dg.DynamicPartitionsDefinition` + a sensor that adds partitions and yields `RunRequest(partition_key=...)` | Each item gets its own history and its own retries |
| Parallel work inside one run that ends in one output | `@dg.graph_asset` with `@dg.op(out=dg.DynamicOut())`, `.map(...)`, `.collect()` | Mirrors `.expand()` + a reduce task |
| A few cheap items | A loop inside one asset | Simplest; per-item retry doesn't matter |

`.partial(...)` constants become plain arguments or resource fields. In the sensor, check
`context.instance.has_dynamic_partition(...)` so items are not re-added on every tick.

## TaskGroups and SubDAGs

- TaskGroup → `group_name="<group>"` (UI grouping), or `key_prefix=["<group>"]` if names
  would collide.
- SubDAG → flatten into the parent's assets; SubDAGs only existed for grouping.

## Cross-DAG dependencies

| Airflow | Upstream also migrated? | Dagster |
|---|---|---|
| `ExternalTaskSensor` | Yes | Depend on the upstream asset (`deps=[dg.AssetKey("orders_table")]`) and set `automation_condition=dg.AutomationCondition.eager()` so it runs when the upstream updates. Delete the sensor. |
| `ExternalTaskSensor` | No (still in Airflow) | `@dg.sensor` that checks the Airflow REST API, or peer the Airflow instance with dagster-airlift and depend on its observed assets |
| Datasets: `outlets=[Dataset(x)]`, `schedule=[Dataset(x)]` | — | The Dataset is an asset key. Depend on it, plus `AutomationCondition.eager()` |
| `TriggerDagRunOperator` | — | Invert it: the downstream asset depends on this one and uses an automation condition, or use `@dg.run_status_sensor` to launch the downstream job |

`AutomationCondition` needs the Dagster daemon to be running (`dagster dev` starts it) and
automation to be enabled for the asset's code location.
