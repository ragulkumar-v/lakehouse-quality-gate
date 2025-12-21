"""
Structural tests for dags/lakehouse_quality_gate_dag.py.

Guarded with `pytest.importorskip("airflow")`: the default offline test
environment (requirements.txt) does NOT include apache-airflow (see
requirements-airflow.txt for why), so this file is skipped automatically
there, and only runs when explicitly invoked against `.venv-airflow`:

    AIRFLOW_HOME=.airflow_home .venv-airflow/bin/python -m pytest tests/airflow/test_dag_structure.py -q

No scheduler, metadata database, or webserver is started -- this only
imports the DAG module and inspects the resulting in-memory `DAG` object,
which is exactly what "unit-testable without a running scheduler" means for
the DAG file itself.
"""

from __future__ import annotations

import pytest

pytest.importorskip(
    "airflow",
    reason="apache-airflow is intentionally not in requirements.txt -- see requirements-airflow.txt",
)


@pytest.fixture
def dag():
    import dags.lakehouse_quality_gate_dag as dag_module

    return dag_module.dag


def test_dag_id(dag):
    assert dag.dag_id == "lakehouse_quality_gate"


def test_dag_has_all_five_tasks(dag):
    expected = {"land_raw_data", "dbt_run_staging", "quality_gate", "dbt_run_marts", "dbt_docs_generate"}
    assert {t.task_id for t in dag.tasks} == expected


def test_dag_is_a_single_linear_chain(dag):
    order = ["land_raw_data", "dbt_run_staging", "quality_gate", "dbt_run_marts", "dbt_docs_generate"]
    for upstream, downstream in zip(order, order[1:], strict=False):
        assert dag.task_dict[downstream].task_id in dag.task_dict[upstream].downstream_task_ids

    # First/last tasks have no upstream/downstream, respectively.
    assert dag.task_dict[order[0]].upstream_task_ids == set()
    assert dag.task_dict[order[-1]].downstream_task_ids == set()


def test_dag_has_no_cycles(dag):
    """dag.tree_view()/topological_sort() raises if the graph has a cycle."""
    topo_order = [t.task_id for t in dag.topological_sort()]
    assert len(topo_order) == len(dag.tasks)


def test_dag_is_not_scheduled_to_catch_up(dag):
    assert dag.catchup is False


def test_chaos_mode_param_offers_every_declared_mode(dag):
    # Hardcoded rather than imported from chaos.inject_chaos: that module
    # (transitively) needs pyarrow, which lives only in the data/dbt
    # virtualenv (requirements.txt), not the Airflow one
    # (requirements-airflow.txt) that runs this file -- see both files'
    # docstrings for why the two are kept separate.
    expected_modes = {"none", "null_flood", "schema_drift", "dup_pk", "negative_values"}
    param = dag.params.get_param("chaos_mode")
    assert set(param.schema.get("enum", [])) == expected_modes
