"""
Airflow DAG: lakehouse_quality_gate

    land_raw_data >> dbt_run_staging >> quality_gate >> dbt_run_marts >> dbt_docs

Every task's actual logic lives in `dags/tasks.py`, which has no dependency
on Airflow at all -- this module's only job is to wire those plain callables
into `PythonOperator`s inside a `DAG`. That split is what makes the pipeline
"importable and unit-testable without a running scheduler": tests exercise
`dags/tasks.py` directly (see tests/airflow/test_tasks.py, which needs
nothing but this project's normal test dependencies), while this file --
which DOES require `apache-airflow` to import -- is covered separately by
tests/airflow/test_dag_structure.py, guarded with
`pytest.importorskip("airflow")` so the default (Airflow-free) test run
skips it cleanly instead of failing on a missing dependency.

Requires: pip install -r requirements-airflow.txt (kept separate from
requirements.txt -- apache-airflow's own dependency pins conflict with
dbt-core's on a handful of transitive packages, so this project runs dbt/GE
and Airflow in two separate virtualenvs, exactly like most real dbt+Airflow
shops that pin dbt inside its own container/venv rather than airflow's).

`chaos_mode` is exposed as an Airflow Param purely for local/manual demoing
("Trigger DAG w/ config" -> {"chaos_mode": "null_flood"}) -- production
scheduled runs always use the default "none".
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from dags.tasks import (
    dbt_docs_task,
    dbt_run_marts_task,
    dbt_run_staging_task,
    land_raw_data_task,
    quality_gate_task,
)

default_args = {
    "owner": "data-platform",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lakehouse_quality_gate",
    description="Land trip data, gate it with Great Expectations, then build marts -- only on a clean bill of health.",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "data-quality", "dbt", "great-expectations"],
    params={
        "chaos_mode": Param(
            "none",
            type="string",
            enum=["none", "null_flood", "schema_drift", "dup_pk", "negative_values"],
            description="Corrupt the landed data on purpose, to demo the quality gate blocking a run.",
        ),
    },
) as dag:
    land_raw_data = PythonOperator(
        task_id="land_raw_data",
        python_callable=land_raw_data_task,
        op_kwargs={"chaos_mode": "{{ params.chaos_mode }}"},
    )

    dbt_run_staging = PythonOperator(
        task_id="dbt_run_staging",
        python_callable=dbt_run_staging_task,
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate_task,
    )

    dbt_run_marts = PythonOperator(
        task_id="dbt_run_marts",
        python_callable=dbt_run_marts_task,
    )

    dbt_docs = PythonOperator(
        task_id="dbt_docs_generate",
        python_callable=dbt_docs_task,
    )

    land_raw_data >> dbt_run_staging >> quality_gate >> dbt_run_marts >> dbt_docs
