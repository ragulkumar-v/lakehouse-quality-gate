"""
End-to-end proof: `scripts.run_pipeline.run_pipeline` -- the same function
used by the Airflow DAG's tasks and the CLI -- behaves exactly as promised
for every chaos mode, entirely offline against DuckDB + the bundled CSV
fixtures.

    none / clean data       -> exit 0, marts get built
    null_flood/dup_pk/
    negative_values         -> exit 1, quality gate blocks it, marts NOT built
    schema_drift            -> exit 2, dbt itself fails before the gate runs
"""

from __future__ import annotations

import duckdb
import pytest

from scripts.run_pipeline import run_pipeline


def _marts_exist(duckdb_path) -> bool:
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        row = con.execute(
            "select count(*) from information_schema.tables where table_schema = 'main_marts'"
        ).fetchone()
        return row[0] > 0
    finally:
        con.close()


def test_clean_run_succeeds_through_marts(isolated_lake):
    exit_code = run_pipeline(chaos_mode="none", duckdb_path=isolated_lake.duckdb_path, skip_docs=True)

    assert exit_code == 0
    assert _marts_exist(isolated_lake.duckdb_path)


@pytest.mark.parametrize("chaos_mode", ["null_flood", "dup_pk", "negative_values"])
def test_semantic_chaos_is_blocked_by_the_quality_gate_before_marts_build(isolated_lake, chaos_mode):
    exit_code = run_pipeline(chaos_mode=chaos_mode, duckdb_path=isolated_lake.duckdb_path, skip_docs=True)

    assert exit_code == 1, f"{chaos_mode} should be blocked by the GE gate (exit 1), got {exit_code}"
    assert not _marts_exist(isolated_lake.duckdb_path), (
        f"marts must NOT be built when the gate blocks {chaos_mode}"
    )


def test_structural_chaos_fails_before_the_gate_even_runs(isolated_lake):
    exit_code = run_pipeline(chaos_mode="schema_drift", duckdb_path=isolated_lake.duckdb_path, skip_docs=True)

    assert exit_code == 2, "schema_drift should fail dbt itself (exit 2), before the GE gate ever runs"
    assert not _marts_exist(isolated_lake.duckdb_path)


def test_alert_payload_is_dispatched_on_a_blocked_run(isolated_lake, tmp_path, monkeypatch):
    """The webhook alert (local-file sink, since GE_WEBHOOK_URL is unset) actually gets written."""
    monkeypatch.delenv("GE_WEBHOOK_URL", raising=False)

    # quality.checkpoint's default sink lives under REPO_ROOT/data/alerts; redirect it for this test.
    import quality.webhook as webhook_module

    sink = tmp_path / "alerts" / "last_alert.json"
    monkeypatch.setattr(webhook_module, "DEFAULT_ALERT_SINK", sink)

    exit_code = run_pipeline(chaos_mode="null_flood", duckdb_path=isolated_lake.duckdb_path, skip_docs=True)

    assert exit_code == 1
    assert sink.exists()
