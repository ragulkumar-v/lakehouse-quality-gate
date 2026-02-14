"""
Task callables for the `lakehouse_quality_gate` Airflow DAG.

Deliberately has ZERO import of `airflow` anywhere in this module. That is
the whole point: every function here is a plain, synchronous, side-effecting
Python callable that Airflow's `PythonOperator` can wrap directly --

    PythonOperator(task_id="land_raw_data", python_callable=land_raw_data_task)

-- but which can also be imported and called directly from a test process
that has never heard of Airflow (see tests/airflow/test_tasks.py). That's
what "importable and unit-testable without a running scheduler" means in
practice: you don't need `airflow db init`, a metadata database, a
scheduler, or a webserver to prove this logic is correct. You just call the
functions.

`dags/lakehouse_quality_gate_dag.py` is the thin layer that imports this
module *and* airflow, and wires these callables into an actual `DAG` object.
That module does need Airflow installed to import -- see requirements-airflow.txt
-- but it contains no logic of its own worth unit testing beyond "does the
graph have the edges we intend", which tests/airflow/test_dag_structure.py
covers (skipped automatically when Airflow isn't installed).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"
DEFAULT_DUCKDB_PATH = REPO_ROOT / "data" / "warehouse.duckdb"


def _duckdb_path() -> Path:
    return Path(os.environ.get("DUCKDB_PATH", DEFAULT_DUCKDB_PATH))


def land_raw_data_task(chaos_mode: str = "none", **_context: Any) -> dict[str, Any]:
    """Airflow task 1: land raw source data (real or chaos-corrupted) into the lake."""
    from ingestion.land_raw_data import land

    manifest = land(chaos_mode=chaos_mode)
    return {
        "trips_rows": manifest.trips_rows,
        "zones_rows": manifest.zones_rows,
        "chaos_mode": manifest.chaos_mode,
    }


def dbt_run_staging_task(**_context: Any) -> None:
    """
    Airflow task 2: build the staging + intermediate dbt layers.

    Structural schema drift (a column renamed/removed upstream) fails HERE,
    loudly, as a dbt runtime error -- before the quality gate task ever
    runs. That is intentional; see quality/checkpoint.py's module docstring.
    """
    from scripts.run_pipeline import PipelineStageError, _dbt

    try:
        _dbt("run", "--select", "staging intermediate")
    except PipelineStageError as exc:
        raise RuntimeError(f"dbt staging/intermediate build failed: {exc}") from exc


def quality_gate_task(**_context: Any) -> dict[str, Any]:
    """
    Airflow task 3: the active quality gate.

    Raises `quality.checkpoint.QualityGateFailure` on failure -- Airflow will
    mark this task (and, by dependency, `dbt_run_marts_task`/`dbt_docs_task`)
    as failed, and a webhook alert has already been dispatched by the time
    the exception propagates. This is exactly the "fails loudly" behavior
    the project exists to demonstrate.
    """
    from quality.checkpoint import run_checkpoint

    result = run_checkpoint(duckdb_path=_duckdb_path())
    return result.to_alert_payload()


def dbt_run_marts_task(**_context: Any) -> None:
    """Airflow task 4: build marts + run dbt data tests. Only reached if the gate passed."""
    from scripts.run_pipeline import PipelineStageError, _dbt

    try:
        _dbt("build", "--select", "marts")
    except PipelineStageError as exc:
        raise RuntimeError(f"dbt marts build failed: {exc}") from exc


def dbt_docs_task(**_context: Any) -> None:
    """Airflow task 5: regenerate dbt docs (catalog.json/manifest.json) for the docs site."""
    from scripts.run_pipeline import PipelineStageError, _dbt

    try:
        _dbt("docs", "generate")
    except PipelineStageError as exc:
        raise RuntimeError(f"dbt docs generate failed: {exc}") from exc
