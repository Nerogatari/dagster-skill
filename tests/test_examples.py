"""Each test pins a claim SKILL.md or references/ makes about Airflow -> Dagster behavior."""

import sqlite3
from datetime import datetime, timezone

import dagster as dg
import pytest

import basic_etl
import branching
import dynamic_fanout
import operations
import partitioned_daily


@pytest.mark.parametrize(
    "module", [basic_etl, partitioned_daily, branching, dynamic_fanout, operations]
)
def test_definitions_load(module, monkeypatch):
    monkeypatch.setenv("WAREHOUSE_PATH", ":memory:")
    module.defs.resolve_all_job_defs()
    module.defs.resolve_asset_graph()


def test_basic_etl_loads_table_and_check_passes(tmp_path):
    db = str(tmp_path / "wh.db")
    result = dg.materialize(
        [basic_etl.raw_orders, basic_etl.orders_table, basic_etl.orders_not_empty],
        resources={"warehouse": basic_etl.Warehouse(path=db)},
    )
    assert result.success
    (check,) = result.get_asset_check_evaluations()
    assert check.passed
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 2


def test_partition_key_and_window_match_airflow_ds_and_interval(tmp_path):
    db = str(tmp_path / "wh.db")
    resources = {"warehouse": partitioned_daily.Warehouse(path=db)}
    for _ in range(2):  # rerun the same day: must stay idempotent
        assert dg.materialize(
            [partitioned_daily.daily_sales], partition_key="2024-01-01", resources=resources
        ).success
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT * FROM daily_sales").fetchall()
    assert rows == [("2024-01-01", "2024-01-01T00:00:00+00:00", "2024-01-02T00:00:00+00:00")]


def test_partitioned_schedule_requests_previous_day_like_airflow():
    # Airflow runs ds=2024-01-01 at 2024-01-02 00:00; the Dagster schedule must too.
    schedule = partitioned_daily.defs.resolve_schedule_def("daily_sales_job_schedule")
    assert schedule.cron_schedule == "0 0 * * *"
    with dg.instance_for_test() as instance:
        ctx = dg.build_schedule_context(
            instance=instance,
            scheduled_execution_time=datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc),
        )
        (request,) = schedule.evaluate_tick(ctx).run_requests
    assert request.partition_key == "2024-01-01"


def test_depends_on_past_maps_to_self_dependency():
    graph = partitioned_daily.defs.resolve_asset_graph()
    key = dg.AssetKey("cumulative_sales")
    assert key in graph.get(key).parent_keys


def _materialized(result: dg.ExecuteInProcessResult) -> set[str]:
    return {e.asset_key.to_user_string() for e in result.get_asset_materialization_events()}


def test_branch_only_chosen_output_materializes_and_downstream_skips():
    result = dg.materialize(
        [branching.choose_load, branching.monthly_report, branching.load_audit]
    )
    assert result.success
    assert _materialized(result) == {"daily_delta", "load_log", "load_audit"}


def test_join_on_both_branches_is_skipped():
    # The pitfall the always-produced load_log output avoids.
    @dg.asset(deps=["monthly_snapshot", "daily_delta"])
    def naive_join() -> None: ...

    result = dg.materialize([branching.choose_load, naive_join])
    assert result.success
    assert "naive_join" not in _materialized(result)


def test_short_circuit_skips_downstream():
    result = dg.materialize([branching.new_data, branching.publish])
    assert result.success
    assert _materialized(result) == set()


def test_dynamic_partitions_sensor_adds_partitions_once():
    with dg.instance_for_test() as instance:
        sensor = dynamic_fanout.new_files_sensor
        first = sensor(dg.build_sensor_context(instance=instance))
        assert [r.partition_key for r in first.run_requests] == ["a.csv", "b.csv"]
        instance.add_dynamic_partitions("files", ["a.csv", "b.csv"])
        assert sensor(dg.build_sensor_context(instance=instance)).run_requests == []


def test_dynamic_out_fans_out_in_one_run():
    result = dg.materialize([dynamic_fanout.files_summary])
    assert result.success
    assert result.output_for_node("files_summary") == len("a.csv") + len("b.csv")


def test_file_sensor_cursor_prevents_double_trigger(tmp_path, monkeypatch):
    flag = tmp_path / "ready.flag"
    monkeypatch.setattr(operations, "DROP_FLAG", flag)
    sensor = operations.drop_ready_sensor
    assert isinstance(sensor(dg.build_sensor_context()), dg.SkipReason)
    flag.write_text("x")
    ctx = dg.build_sensor_context()
    assert isinstance(sensor(ctx), dg.RunRequest)
    assert isinstance(sensor(dg.build_sensor_context(cursor=ctx.cursor)), dg.SkipReason)


def test_operational_settings_are_attached():
    spec = operations.warehouse_report
    assert spec.op.retry_policy.max_retries == 3
    assert spec.op.retry_policy.backoff == dg.Backoff.EXPONENTIAL
    assert spec.op.pool == "warehouse"
    assert operations.report_job.tags["dagster/max_runtime"] == "3600"
