"""
Tests for quality/checkpoint.py -- the active Great Expectations gate.

These are the tests that matter most for this project's thesis: a
structurally-valid-but-semantically-corrupted `int_trips_enriched` batch
must be BLOCKED (gate_result.success is False, QualityGateFailure raises,
and a webhook alert payload naming the exact failure is dispatched) while a
clean batch sails through.
"""

from __future__ import annotations

import json

import pytest

from quality.checkpoint import GATED_MODEL, GATED_SCHEMA, QualityGateFailure, run_checkpoint
from quality.webhook import WebhookNotifier


def test_clean_data_passes_the_gate(clean_staged_lake):
    result = run_checkpoint(duckdb_path=clean_staged_lake.duckdb_path)
    assert result.success is True
    assert result.failures == []
    assert result.row_count == 2000
    assert result.model == f"{GATED_SCHEMA}.{GATED_MODEL}"


@pytest.mark.parametrize(
    "chaos_mode,expected_expectation_types,expected_columns",
    [
        pytest.param(
            "null_flood",
            {"expect_column_values_to_not_be_null"},
            {"fare_amount"},
            id="null_flood-blocked-on-fare_amount",
        ),
        pytest.param(
            "dup_pk",
            {"expect_column_values_to_be_unique"},
            {"trip_id"},
            id="dup_pk-blocked-on-trip_id-uniqueness",
        ),
        pytest.param(
            "negative_values",
            {"expect_column_values_to_be_between"},
            {"fare_amount", "trip_distance_miles"},
            id="negative_values-blocked-on-range-checks",
        ),
    ],
)
def test_gate_blocks_corrupted_fixture(
    isolated_lake, chaos_mode, expected_expectation_types, expected_columns
):
    """
    THE proof: land chaos-corrupted data, build staging+intermediate (which
    succeeds -- the corruption is semantic, not structural), then assert the
    quality gate refuses to let it through, naming exactly the right failing
    expectation(s) and columns.
    """
    isolated_lake.land(chaos_mode=chaos_mode)
    isolated_lake.dbt("run", "--select", "staging intermediate")

    with pytest.raises(QualityGateFailure) as excinfo:
        run_checkpoint(duckdb_path=isolated_lake.duckdb_path)

    payload = excinfo.value.payload
    assert payload["success"] is False
    assert payload["failed_expectation_count"] >= 1

    seen_types = {f["expectation_type"] for f in payload["failures"]}
    seen_columns = {f["column"] for f in payload["failures"]}
    assert expected_expectation_types <= seen_types
    assert expected_columns <= seen_columns

    # Every failure must list at least one concrete offending trip_id --
    # that's the whole point of "listing the failing rows", not just a count.
    for failure in payload["failures"]:
        assert failure["unexpected_count"] > 0
        assert len(failure["sample_trip_ids"]) > 0


def test_gate_failure_still_reports_when_raise_on_failure_is_false(isolated_lake):
    isolated_lake.land(chaos_mode="null_flood")
    isolated_lake.dbt("run", "--select", "staging intermediate")

    result = run_checkpoint(duckdb_path=isolated_lake.duckdb_path, raise_on_failure=False)

    assert result.success is False
    assert len(result.failures) >= 1


def test_gate_failure_dispatches_a_webhook_alert(isolated_lake, tmp_path):
    isolated_lake.land(chaos_mode="null_flood")
    isolated_lake.dbt("run", "--select", "staging intermediate")

    sink = tmp_path / "custom_alert_sink.json"
    notifier = WebhookNotifier(sink_path=sink)

    with pytest.raises(QualityGateFailure):
        run_checkpoint(duckdb_path=isolated_lake.duckdb_path, notifier=notifier)

    assert sink.exists()
    alert = json.loads(sink.read_text())
    assert alert["success"] is False
    assert alert["gate"] == "great_expectations_checkpoint"
    assert alert["failed_expectation_count"] >= 1
    assert any(f["column"] == "fare_amount" for f in alert["failures"])


def test_clean_data_does_not_dispatch_an_alert(clean_staged_lake, tmp_path):
    sink = tmp_path / "should_not_exist.json"
    notifier = WebhookNotifier(sink_path=sink)

    run_checkpoint(duckdb_path=clean_staged_lake.duckdb_path, notifier=notifier)

    assert not sink.exists()


def test_missing_warehouse_raises_a_clear_error(tmp_path):
    with pytest.raises(FileNotFoundError):
        run_checkpoint(duckdb_path=tmp_path / "does_not_exist.duckdb")
