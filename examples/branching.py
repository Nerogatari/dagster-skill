"""BranchPythonOperator / ShortCircuitOperator -> plain `if`, or optional outputs.

Airflow original:

    choose = BranchPythonOperator(task_id="choose", python_callable=lambda: (
        "full_refresh" if is_first_of_month() else "incremental"))
    has_new_data = ShortCircuitOperator(task_id="has_new_data", python_callable=new_data_exists)
    choose >> [full_refresh, incremental]
    has_new_data >> publish

Rule of thumb:
- Both branches build the SAME data in different ways -> one asset with an `if`.
- Branches build DIFFERENT data -> multi_asset with optional outputs.
- ShortCircuit (skip the rest) -> asset with output_required=False that yields nothing.
Assets downstream of an output that was not produced are skipped in that run.

Join after a branch (trigger_rule="none_failed_min_one_success") has no direct
equivalent: an asset depending on both optional outputs is SKIPPED, because one of
them never ran. Make the join depend on an output that is always produced.
"""

import dagster as dg


def is_first_of_month() -> bool:
    return False


def new_data_exists() -> bool:
    return False


# Same data either way: the branch is just an `if` inside one asset.
@dg.asset
def orders_rollup() -> dg.MaterializeResult:
    mode = "full_refresh" if is_first_of_month() else "incremental"
    return dg.MaterializeResult(metadata={"mode": mode})


# Different data per branch: only the chosen output materializes.
@dg.multi_asset(
    outs={
        "monthly_snapshot": dg.AssetOut(is_required=False),
        "daily_delta": dg.AssetOut(is_required=False),
        "load_log": dg.AssetOut(),  # always produced, so a join can depend on it
    },
    can_subset=False,
)
def choose_load():
    if is_first_of_month():
        yield dg.Output(None, output_name="monthly_snapshot")
    else:
        yield dg.Output(None, output_name="daily_delta")
    yield dg.Output(None, output_name="load_log")


@dg.asset(deps=["monthly_snapshot"])
def monthly_report() -> None:
    """Skipped in runs where choose_load took the daily branch."""


@dg.asset(deps=["load_log"])
def load_audit() -> None:
    """The join: runs whichever branch was taken. deps=[both branches] would skip it."""


# ShortCircuitOperator: yield nothing to skip everything downstream.
@dg.asset(output_required=False)
def new_data():
    if new_data_exists():
        yield dg.Output(None)


@dg.asset(deps=[new_data])
def publish() -> None:
    """Skipped when new_data produced no output."""


defs = dg.Definitions(
    assets=[orders_rollup, choose_load, monthly_report, load_audit, new_data, publish]
)
