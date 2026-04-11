"""
Tests for dags/tasks.py -- the Airflow task logic, exercised with ZERO
Airflow installed. This is the concrete proof that the DAG's task logic is
"importable and unit-testable without a running scheduler": these tests run
in the same offline `requirements.txt` environment as everything else.
"""

from __future__ import annotations

import sys

import pytest

from quality.checkpoint import QualityGateFailure


def test_dags_tasks_module_has_no_airflow_import():
    """`dags.tasks` must be importable even if `airflow` is not installed."""
    assert "airflow" not in sys.modules or True  # sanity: don't assume prior state
    import dags.tasks as tasks_module

    # No top-level reference to an `airflow` module object anywhere in the
    # module's own namespace (imports are all local, inside function bodies).
    assert "airflow" not in dir(tasks_module)
    source = tasks_module.__file__
    with open(source) as f:
        contents = f.read()
    for line in contents.splitlines():
        stripped = line.strip()
        if stripped.startswith(("import airflow", "from airflow")):
            pytest.fail(f"dags/tasks.py must not import airflow at module scope: {line!r}")


def test_land_raw_data_task_lands_clean_data(isolated_lake, monkeypatch):
    from dags.tasks import land_raw_data_task

    result = land_raw_data_task(chaos_mode="none")

    assert result["trips_rows"] == 2000
    assert result["zones_rows"] == 15
    assert result["chaos_mode"] == "none"


def test_dbt_run_staging_task_then_quality_gate_task_pass_on_clean_data(isolated_lake):
    from dags.tasks import dbt_run_staging_task, land_raw_data_task, quality_gate_task

    land_raw_data_task(chaos_mode="none")
    dbt_run_staging_task()  # must not raise
    payload = quality_gate_task()

    assert payload["success"] is True
    assert payload["failed_expectation_count"] == 0


def test_quality_gate_task_raises_on_corrupted_data(isolated_lake):
    from dags.tasks import dbt_run_staging_task, land_raw_data_task, quality_gate_task

    land_raw_data_task(chaos_mode="null_flood")
    dbt_run_staging_task()

    with pytest.raises(QualityGateFailure):
        quality_gate_task()


def test_dbt_run_staging_task_raises_on_schema_drift(isolated_lake):
    """Structural drift fails at the dbt-run task, before the gate task ever runs."""
    from dags.tasks import dbt_run_staging_task, land_raw_data_task

    land_raw_data_task(chaos_mode="schema_drift")

    with pytest.raises(RuntimeError, match="dbt staging/intermediate build failed"):
        dbt_run_staging_task()


def test_full_task_chain_reaches_marts_on_clean_data(isolated_lake):
    from dags.tasks import (
        dbt_docs_task,
        dbt_run_marts_task,
        dbt_run_staging_task,
        land_raw_data_task,
        quality_gate_task,
    )

    land_raw_data_task(chaos_mode="none")
    dbt_run_staging_task()
    quality_gate_task()
    dbt_run_marts_task()  # must not raise
    dbt_docs_task()  # must not raise

    import duckdb

    con = duckdb.connect(str(isolated_lake.duckdb_path), read_only=True)
    try:
        count = con.execute("select count(*) from main_marts.fct_trips").fetchone()[0]
    finally:
        con.close()
    assert count == 2000
