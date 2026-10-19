# airflow-to-dagster skill guide

Last updated 2026-10-18.

## What the skill is

The airflow-to-dagster skill teaches an AI coding agent (Claude Code or a compatible agent) to port Apache Airflow DAGs to native Dagster assets **without changing what the pipeline does**. It is published at [github.com/Nerogatari/dagster-skill](https://github.com/Nerogatari/dagster-skill).

The skill counts a migration as done when Dagster does the same work, on the same data, at the same times, with the same failure handling. Rewriting tasks as assets is the easy part. The bugs come from behavior that Airflow handled without anyone writing it down:

| Silent breakage | What goes wrong | How the skill handles it |
| --- | --- | --- |
| Off-by-one interval | The Airflow run for `ds=2024-01-01` starts on 2024-01-02. A plain cron schedule that computes "yesterday" by hand processes the wrong day once anything reruns. | Partitioned assets plus `build_schedule_from_partitioned_job`, which requests the previous day (tested) |
| Lost data handoff | `deps=[...]` passes no data, so XCom values disappear | An explicit choice per edge: I/O manager or external storage |
| Skipped join | An asset that depends on both branch outputs is skipped when only one branch ran | The join depends on an output that is always produced (tested) |
| Lost alerting | `on_failure_callback` and `email_on_failure` vanish | Run failure sensors |
| Timezone drift | Airflow uses the `start_date` timezone | Same `timezone=` on the partitions or schedule |
| Duplicate rows | Retries and backfills rerun appends | Delete-then-insert or `MERGE` per partition first |

**Out of scope:** writing new Dagster code with no Airflow source, comparing the two tools, Airflow 2 → 3 upgrades, and migrations from other orchestrators such as Prefect. For many DAGs that must keep running during cutover, the skill points to [dagster-airlift](https://dagster.io/docs/migration/airflow-to-dagster) instead of a native rewrite.

## How to install and use it

Clone the repo into your skills folder. The agent then loads the skill on its own whenever a request is about moving Airflow work to Dagster.

```sh
# Personal skill, available in every project
git clone https://github.com/Nerogatari/dagster-skill.git ~/.claude/skills/airflow-to-dagster

# Or project-only: commit it under the repo
git clone https://github.com/Nerogatari/dagster-skill.git .claude/skills/airflow-to-dagster
```

Restart the agent session after installing. To use the skill on purpose, name it ("use the airflow-to-dagster skill to port this DAG") or just describe the task.

### Two kinds of request

The skill sizes its answer to the request:

| Request | Example prompt | What you get |
| --- | --- | --- |
| Concept question | "What replaces `{{ ds }}` and catchup in Dagster?" | The mapping and one snippet. No diagrams and no checklist. |
| Full DAG conversion | "Port `dags/orders_etl.py` to Dagster assets." | The full output, in the order below |

For a full conversion, the output comes in this order:

1. **Inventory table:** each Airflow item found in the DAG (schedule, templated fields, XCom edges, branches, operators, retries, callbacks) and the Dagster decision for it.
2. **Two Mermaid diagrams:** the original DAG, then the asset graph, with each edge labeled by how data moves.
3. **Code:** complete and runnable, ending in a `dg.Definitions` that registers every asset, check, job, schedule, sensor and resource.
4. **Tests:** `dg.materialize` per asset, the schedule evaluated at a known tick, and sensors checked for double triggers.
5. **Behavior changes and open questions:** anything that differs from Airflow or rests on an assumption.
6. **Checklist:** only the items that apply.

### Tips for better results

- Give the agent the DAG file itself, plus any SQL or scripts it templates. The inventory step reads them.
- Say whether upstream DAGs are also being migrated. That decides how cross-DAG sensors are replaced.
- Answer the open questions the agent raises, such as whether a task is idempotent or whether both branches write the same table. The skill tells it to ask rather than guess.
- Run the generated tests, and if possible compare the output with an Airflow run for the same `ds`.

## How the skill is organized

`SKILL.md` is always loaded. The agent reads the reference files only when the DAG needs them, which keeps the context small.

| Path | What it holds | When the agent uses it |
| --- | --- | --- |
| `SKILL.md` | Goal, path choice (native rewrite vs dagster-airlift), the four-step workflow, concept map, top 8 pitfalls, output format | Every request |
| `references/scheduling.md` | Logical date, the `{{ ds }}` template variables mapped to context, partitions per schedule, catchup, `depends_on_past`, `max_active_runs` | The DAG has a schedule or uses its interval |
| `references/control-flow.md` | Branching, trigger rules, the join-after-branch fix, `.expand()`, TaskGroups, cross-DAG sensors and Datasets | Branches, mapping or links to other DAGs |
| `references/operators.md` | Python, Bash (Pipes), Kubernetes, SQL, dbt, hooks, Connections, Variables, sensors, notifications | Any operator other than plain `@task` |
| `references/operations.md` | Retries, pools, timeouts, callbacks, SLAs, owners, asset checks | `default_args` or task-level settings |
| `examples/*.py` | Runnable Dagster code for each pattern | Copied from when writing code |
| `tests/test_examples.py` | 16 pytest tests that pin each behavior the skill claims | Maintainers and CI |
| `evals/` | Prompt evals and trigger evals | Measuring skill quality |

The workflow in `SKILL.md` has four steps:

1. **Inventory:** list every semantic item in the DAG and record a decision for each.
2. **Decide what becomes an asset:** data-producing tasks become assets named after their data; side effects (notify, cleanup, trigger) go into sensors or the asset that needs them.
3. **Write the code:** copy the tested patterns from `examples/`.
4. **Prove behavior parity:** write tests for the assets, the schedule timing and the sensors.

## How to evaluate

Evaluate the skill at four layers. Each one catches a different failure: wrong patterns, a skill that doesn't load, weak answers, or a real migration that drifts.

| Layer | Question it answers | How | Suggested pass bar |
| --- | --- | --- | --- |
| 1. Pattern tests | Is the code the skill teaches correct on the current Dagster? | `pytest` | 16 of 16 |
| 2. Trigger evals | Does the skill load when it should, and only then? | `evals/triggers.json`, 13 prompts | All 7 positives load; none of the 6 negatives do |
| 3. Output evals | Does the agent's answer meet the skill's rules? | `evals/evals.json`, 7 cases with assertions | Every assertion in every case, and better than a run without the skill |
| 4. Real-DAG parity | Does a migrated DAG behave like the original? | Your own DAG, compared output for the same `ds` | Same rows, same run times, same alerts |

The pass bars are suggestions; tighten them as the evals grow.

### 1. Pattern tests

The tests run each example and assert the behavior the skill claims. Examples: the schedule requests partition `2024-01-01` at 2024-01-02 00:00, a rerun of a partition doesn't duplicate rows, and a naive join after a branch is skipped.

```sh
python -m venv .venv
.venv/bin/pip install -r requirements-dev.txt   # Windows: .venv\Scripts\pip
.venv/bin/pytest
```

GitHub Actions runs the same suite on every push, on Python 3.11 and 3.13. When a test fails after a Dagster upgrade, the skill is teaching something that is no longer true. Fix the example and the matching text in `SKILL.md` or `references/` together.

### 2. Trigger evals

The trigger set includes near-miss prompts that should **not** load the skill, such as "Migrate our Prefect flows to Dagster" and "Upgrade our DAGs from Airflow 2 to Airflow 3".

1. Install the skill, then start a fresh agent session for each prompt in `evals/triggers.json`.
2. Send the prompt and note whether the agent loaded `airflow-to-dagster` (Claude Code shows the Skill call in the transcript).
3. Compare with `should_trigger`. A miss on a positive means the `description` in `SKILL.md` lacks that wording. A load on a negative means the description is too broad.

### 3. Output evals

Each case in `evals/evals.json` has an input prompt and a list of assertions, for example "Uses DailyPartitionsDefinition with timezone='America/New_York'" or "Does not produce Mermaid diagrams for this concept question".

1. Run each prompt twice in fresh sessions: once with the skill installed and once with it removed (the baseline).
2. Grade each assertion as pass or fail for both runs. A second person or a separate grading agent should do this, not the session that wrote the answer.
3. Report the pass rate per case for both runs. The skill earns its place only if it beats the baseline.
4. Run the generated code too. An answer that reads well but fails `dagster dev` or its own tests counts as a fail.

The repo has no automated eval runner yet. You can run the cases by hand, or use the `skill-creator` skill, which can run evals and compare a skill against a baseline.

### 4. Real-DAG parity

The strongest test is a DAG you actually run:

- [ ] Pick a DAG with a schedule, one XCom edge, and retries or a callback.
- [ ] Port it with the skill, and answer its open questions.
- [ ] Run the Dagster asset for a past `ds` that Airflow also ran, then compare the output rows.
- [ ] Evaluate the schedule for the next few ticks and confirm each requests the same interval Airflow would.
- [ ] Force a failure and confirm the alert still arrives.

### Manual review rubric

Use this when reviewing a single conversion:

| Check | Pass when |
| --- | --- |
| Inventory | Every templated field, XCom edge, branch and `default_args` key in the DAG appears in the table |
| Runnable | No `...` placeholders; `dagster dev` loads the `Definitions` |
| Timing | Interval DAGs use partitions, with the DAG's timezone |
| Handoff | Every edge says I/O manager, storage or deps only |
| Operations | Retries, pools, timeouts, alerts and SLAs are carried over or flagged |
| Honesty | Behavior changes and assumptions are listed, not hidden |

## Maintaining the skill

Keep three things in step: the example, its test, and the text that describes it. A claim in `SKILL.md` or `references/` without a test behind it can go stale unnoticed.

- **New Dagster release:** bump `requirements-dev.txt`, run `pytest` with warnings visible, and fix deprecations in the examples. The current suite was verified on Dagster 1.13.26.
- **New pattern** (for example a new operator): add a runnable example, a test that asserts its behavior, a row in the matching reference file, and an eval case if it changes what the agent outputs.
- **Changes:** record each change in `CHANGELOG.md` with its date.

### Known gaps

- Four snippets in the reference files have not been run: the Postgres resource, the Kubernetes Pipes client, the BashOperator Pipes snippet and the strict `depends_on_past` guard. The first two need packages the test environment doesn't install.
- `wait_for_downstream`, `one_success`, `max_retry_delay` and `on_retry_callback` have no Dagster equivalent. The skill flags them as behavior changes.
- The output and trigger evals still run by hand.

## Sources

- [Dagster: migrating from Airflow](https://dagster.io/docs/migration/airflow-to-dagster)
- [Dagster: basic Airflow migration](https://dagster.io/docs/migration/airflow-to-dagster/basic-migration)
- [Dagster: Airflow operator migration](https://dagster.io/docs/migration/airflow-to-dagster/airflow-operator-migration)
- [Skill repository](https://github.com/Nerogatari/dagster-skill)
