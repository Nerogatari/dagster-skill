# Changelog

## 2026-10-18 (v2)
- Reframed the goal as behavior parity (same data, same times, same failure handling), not a code rewrite
- Added a required semantics inventory step, asset-vs-side-effect rules, and "ask or state an assumption" for ambiguous choices
- Scaled output to the request: concept questions get a snippet, full conversions get diagrams, tests and a checklist
- New references: scheduling (logical date, `{{ ds }}` macros, partitions, catchup, depends_on_past, max_active_runs), control flow (branching, trigger rules, the join-after-branch skip, `.expand`, cross-DAG/Datasets), operators (Pipes, K8s, SQL, dbt, hooks, variables, sensors), operations (retries, pools, timeouts, callbacks, SLAs)
- Added runnable `examples/` and `tests/` (pytest, Dagster 1.13) that pin each claim, including Airflow-equivalent schedule timing
- Fixed: placeholder `data = ...` example, asset check on an asset that returns nothing, nonexistent `PostgresResource`, DAG tags mapped to a single `group_name`, ExternalTaskSensor on a migrated DAG mapped to a polling sensor, BashOperator via raw `subprocess`
- Evals: assertions for partitions, branching joins, dynamic mapping and operational settings; near-miss trigger negatives

## 2026-10-18
- Initial skill creation
- Added concept mapping table, core workflow, example conversion, and common pitfalls based on official Dagster migration docs
- Added Visualization for Human Review section (Mermaid diagrams for DAG vs Asset Graph) to support manual logic checks
- Added Common Operator Conversion Patterns (PythonOperator, BashOperator, SQLOperator)
- Added I/O Manager vs External Storage decision guide
- Added Asset Checks recommended usage
- Added Verification Checklist for human review
