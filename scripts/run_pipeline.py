#!/usr/bin/env python
"""
Single, idempotent entrypoint for the full lakehouse pipeline:

    land raw data -> dbt build staging+intermediate -> quality gate
        -> (only if the gate passes) dbt build marts -> dbt docs generate

This is the exact sequence `dags/tasks.py` wires into Airflow tasks; it's
kept here as a standalone script, dependency-light and directly runnable, so
you don't need a scheduler to exercise the whole pipeline end to end:

    python -m scripts.run_pipeline --chaos-mode none
    python -m scripts.run_pipeline --chaos-mode null_flood   # gate blocks this

Exit codes:
    0 -- full pipeline succeeded through marts + docs
    1 -- quality gate blocked the run (alert dispatched; marts NOT built)
    2 -- an earlier stage failed outright (ingestion, or dbt staging/intermediate
         itself errored -- e.g. the `schema_drift` chaos mode, which breaks a
         `cast()` before the quality gate ever gets a chance to run)
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from chaos.inject_chaos import CHAOS_MODES
from ingestion.land_raw_data import land
from quality.checkpoint import QualityGateFailure, run_checkpoint

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"

# The `dbt` console script installed alongside this interpreter -- resolved
# from sys.executable rather than relying on PATH, so this works the same
# whether invoked from an activated venv or straight via `.venv-data/bin/python`.
_DBT_BIN = str(Path(sys.executable).parent / "dbt")


class PipelineStageError(RuntimeError):
    """Raised when a dbt invocation fails outright (before the quality gate runs)."""


def _dbt(*args: str, env: dict[str, str] | None = None) -> None:
    """
    Run a dbt command as a subprocess (not dbt's in-process `dbtRunner`).

    Deliberately a subprocess, not an in-process call: dbt-duckdb caches an
    open DuckDB connection for the life of the Python process, and this
    pipeline's very next step (`quality.checkpoint`) needs its own,
    independent, read-only connection to the same file. DuckDB refuses a
    second same-process connection to one file under a different
    configuration, so keeping dbt in its own process is what makes "dbt
    writes, then GE reads" work reliably -- exactly as it would across two
    separate Airflow tasks in production.
    """
    cmd = [_DBT_BIN, *args, "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)]
    result = subprocess.run(cmd, cwd=REPO_ROOT, env={**os.environ, **(env or {})})
    if result.returncode != 0:
        raise PipelineStageError(
            f"dbt {' '.join(args)} exited with code {result.returncode}; see output above"
        )


def run_pipeline(chaos_mode: str = "none", duckdb_path: Path | None = None, skip_docs: bool = False) -> int:
    """Run the full pipeline. Returns a process exit code (see module docstring)."""
    duckdb_path = duckdb_path or Path(os.environ.get("DUCKDB_PATH", REPO_ROOT / "data" / "warehouse.duckdb"))
    os.environ["DUCKDB_PATH"] = str(duckdb_path)

    print(f"[1/5] Landing raw data (chaos_mode={chaos_mode})...")
    manifest = land(chaos_mode=chaos_mode)
    print(f"      landed {manifest.trips_rows} trips, {manifest.zones_rows} zones")

    print("[2/5] dbt build: staging + intermediate...")
    try:
        _dbt("run", "--select", "staging intermediate")
    except PipelineStageError as exc:
        print(f"BLOCKED at staging/intermediate: {exc}", file=sys.stderr)
        return 2

    print("[3/5] Great Expectations quality gate on intermediate.int_trips_enriched...")
    try:
        gate_result = run_checkpoint(duckdb_path=duckdb_path)
    except QualityGateFailure as exc:
        print(f"BLOCKED by quality gate: {exc}", file=sys.stderr)
        print(json.dumps(exc.payload, indent=2, default=str), file=sys.stderr)
        return 1
    print(f"      gate PASSED: {gate_result.row_count} rows, 0 failed expectations")

    print("[4/5] dbt build: marts (+ data tests)...")
    _dbt("build", "--select", "marts")

    if not skip_docs:
        print("[5/5] dbt docs generate...")
        _dbt("docs", "generate")
    else:
        print("[5/5] dbt docs generate (skipped)")

    print("Pipeline completed successfully.")
    return 0


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--chaos-mode", default="none", choices=sorted(CHAOS_MODES))
    parser.add_argument("--duckdb-path", default=None)
    parser.add_argument("--skip-docs", action="store_true")
    args = parser.parse_args()

    exit_code = run_pipeline(
        chaos_mode=args.chaos_mode,
        duckdb_path=Path(args.duckdb_path) if args.duckdb_path else None,
        skip_docs=args.skip_docs,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    _cli()
