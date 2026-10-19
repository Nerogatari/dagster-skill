# airflow-to-dagster skill

An agent skill (Claude Code and compatible agents) for porting Apache Airflow DAGs to native
Dagster assets while keeping run-time behavior the same: which interval each run processes,
how data moves between steps, what happens after branches, and how failures are retried and
alerted.

## Layout

| Path | Contents |
|---|---|
| `SKILL.md` | The skill: workflow, concept map, pitfalls, output format |
| `references/` | Detail loaded on demand: scheduling, control flow, operators, operations |
| `examples/` | Runnable Dagster code for each migration pattern |
| `tests/` | pytest suite that pins each behavior the skill claims |
| `evals/` | Prompt evals and trigger evals |

## Install

Clone or copy this directory into your skills folder, for example:

```sh
git clone https://github.com/Nerogatari/dagster-skill.git ~/.claude/skills/airflow-to-dagster
```

## Run the tests

```sh
python -m venv .venv
.venv/bin/pip install -r requirements-dev.txt   # Windows: .venv\Scripts\pip
.venv/bin/pytest
```

Verified with Dagster 1.13.26 on Python 3.14.
