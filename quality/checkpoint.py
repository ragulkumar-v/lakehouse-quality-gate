"""
The active quality gate: a Great Expectations checkpoint that sits BETWEEN
dbt's staging/intermediate layers and its marts.

Why here, and not just as dbt `data_tests`? dbt tests are excellent for
"is this what I expect the shape of my data to be" documentation, and this
project has plenty of them (see models/**/*.yml). But dbt tests are informational
by default -- `dbt test` reports failures, it doesn't stop anything from
happening unless you wire `dbt build`'s fail-fast behavior very carefully,
and even then you're still inside dbt's world (SQL assertions only, no
webhook, no structured "here are the exact bad rows" payload for a human or
downstream automation to act on).

This module is the enforcement point: it is invoked as a *separate step* in
the pipeline (see dags/tasks.py / scripts/run_pipeline.py) after
`intermediate.int_trips_enriched` is built and before `dbt run --select
marts` is ever allowed to execute. If validation fails, it:

  1. Raises `QualityGateFailure` -- callers (the Airflow task, the CLI
     script) MUST let this propagate and MUST NOT build marts afterwards.
  2. Sends a structured alert payload -- via `quality.webhook.WebhookNotifier`
     -- naming every failed expectation and a sample of the exact offending
     `trip_id`s, so an on-call engineer (or PagerDuty/Slack automation) knows
     precisely what broke without re-running anything.

Two independent layers of "schema drift or null-floods make pipelines
silently break" are covered by this project, deliberately:

  * STRUCTURAL drift (a column disappears/is renamed upstream) is caught by
    dbt itself, immediately and loudly, when it tries to `cast()` a column
    that no longer exists -- see chaos/inject_chaos.py's `schema_drift` mode
    and tests/chaos/test_gate_catches_chaos.py. That failure happens before
    this module ever runs.
  * SEMANTIC drift (the columns are all still there, but the values are
    wrong -- null floods, duplicate keys, impossible negative values) is
    exactly what this checkpoint exists to catch, because that kind of
    corruption sails straight through a `CREATE VIEW` and even a `dbt test`
    that only checks structural things.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import great_expectations as gx
import pandas as pd

from quality.expectations.int_trips_enriched import SUITE_NAME, build_suite
from quality.webhook import WebhookNotifier

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DUCKDB_PATH = REPO_ROOT / "data" / "warehouse.duckdb"

# dbt-duckdb prefixes a model's configured `+schema` with the profile's base
# schema (here "main", from profiles.yml) -- so the `intermediate` custom
# schema in dbt_project.yml actually lands as `main_intermediate` in DuckDB.
# See https://github.com/duckdb/dbt-duckdb -- this is dbt-duckdb's standard
# (and, for multi-tenant safety, intentional) schema-generation behavior.
GATED_SCHEMA = "main_intermediate"
GATED_MODEL = "int_trips_enriched"

# How many offending trip_ids to include per failed expectation in the alert
# payload. Kept small -- this is an alert, not a data export.
SAMPLE_ROWS_PER_FAILURE = 10


class QualityGateFailure(RuntimeError):
    """
    Raised when the checkpoint blocks the run.

    Carries the full alert payload (`.payload`) so callers that catch this
    (e.g. an Airflow task wrapper that wants to log structured failure
    details before re-raising) don't have to recompute anything.
    """

    def __init__(self, message: str, payload: dict[str, Any]):
        super().__init__(message)
        self.payload = payload


@dataclass
class ExpectationFailure:
    expectation_type: str
    column: str | None
    unexpected_count: int
    unexpected_percent: float
    sample_trip_ids: list[Any]
    notes: str


@dataclass
class GateResult:
    success: bool
    checked_at: str
    row_count: int
    suite_name: str
    model: str
    failures: list[ExpectationFailure] = field(default_factory=list)

    def to_alert_payload(self, source: str = "quality.checkpoint") -> dict[str, Any]:
        return {
            "source": source,
            "gate": "great_expectations_checkpoint",
            "model": self.model,
            "suite_name": self.suite_name,
            "checked_at": self.checked_at,
            "success": self.success,
            "row_count": self.row_count,
            "failed_expectation_count": len(self.failures),
            "failures": [asdict(f) for f in self.failures],
        }


def _load_batch_df(duckdb_path: Path) -> pd.DataFrame:
    if not duckdb_path.exists():
        raise FileNotFoundError(
            f"No DuckDB warehouse at {duckdb_path}. Run `dbt build --select staging+intermediate` "
            "(or scripts/run_pipeline.py) before invoking the quality gate."
        )
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        return con.execute(f"select * from {GATED_SCHEMA}.{GATED_MODEL}").fetchdf()
    finally:
        con.close()


def _validate(df: pd.DataFrame) -> tuple[bool, list[ExpectationFailure]]:
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas("quality_gate_pandas")
    data_asset = data_source.add_dataframe_asset(name=GATED_MODEL)
    batch_definition = data_asset.add_batch_definition_whole_dataframe("quality_gate_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = build_suite()
    result = batch.validate(suite, result_format="COMPLETE")

    failures: list[ExpectationFailure] = []
    for expectation_result in result.results:
        if expectation_result.success:
            continue
        config = expectation_result.expectation_config
        column = config.kwargs.get("column")
        detail = expectation_result.result

        sample_trip_ids: list[Any] = []
        unexpected_indices = detail.get("unexpected_index_list") or []
        if "trip_id" in df.columns and unexpected_indices:
            sample_trip_ids = df.loc[
                df.index.intersection(unexpected_indices[:SAMPLE_ROWS_PER_FAILURE]), "trip_id"
            ].tolist()

        failures.append(
            ExpectationFailure(
                expectation_type=config.type,
                column=column,
                unexpected_count=int(detail.get("unexpected_count") or 0),
                unexpected_percent=float(detail.get("unexpected_percent") or 0.0),
                sample_trip_ids=sample_trip_ids,
                notes=(config.meta or {}).get("notes", ""),
            )
        )

    return result.success, failures


def run_checkpoint(
    duckdb_path: Path | str | None = None,
    notifier: WebhookNotifier | None = None,
    raise_on_failure: bool = True,
) -> GateResult:
    """
    Run the Great Expectations checkpoint against `intermediate.int_trips_enriched`.

    Returns a `GateResult` on success. On failure, always sends a webhook
    alert; additionally raises `QualityGateFailure` unless
    `raise_on_failure=False` (used by tests/tooling that want to inspect the
    result without a stack trace).
    """
    resolved_path = Path(duckdb_path or os.environ.get("DUCKDB_PATH") or DEFAULT_DUCKDB_PATH)
    df = _load_batch_df(resolved_path)
    success, failures = _validate(df)

    gate_result = GateResult(
        success=success,
        checked_at=datetime.now(UTC).isoformat(),
        row_count=len(df),
        suite_name=SUITE_NAME,
        model=f"{GATED_SCHEMA}.{GATED_MODEL}",
        failures=failures,
    )

    if not gate_result.success:
        payload = gate_result.to_alert_payload()
        (notifier or WebhookNotifier()).send(payload)
        if raise_on_failure:
            raise QualityGateFailure(
                f"Quality gate BLOCKED the run: {len(failures)} expectation(s) failed against "
                f"{gate_result.model} ({gate_result.row_count} rows checked). "
                f"Alert payload dispatched.",
                payload,
            )

    return gate_result


def _cli() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb-path", default=None)
    parser.add_argument(
        "--no-raise",
        action="store_true",
        help="Report the result as JSON without raising/failing the process on a blocked gate.",
    )
    args = parser.parse_args()

    try:
        result = run_checkpoint(duckdb_path=args.duckdb_path, raise_on_failure=not args.no_raise)
    except QualityGateFailure as exc:
        print(json.dumps(exc.payload, indent=2, default=str))
        sys.exit(1)

    print(json.dumps(result.to_alert_payload(), indent=2, default=str))
    if not result.success:
        sys.exit(1)


if __name__ == "__main__":
    _cli()
