"""
Shared pytest fixtures.

Every fixture here gives each test its own, fully isolated DuckDB warehouse
file, Iceberg lake directory, and catalog DB under `tmp_path` -- nothing
touches this repo's own `data/` directory, and no test depends on another
test having run first. That isolation is what lets the whole suite run
serially, offline, with zero setup beyond `pip install -r requirements.txt`.
"""

from __future__ import annotations

import dataclasses
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = REPO_ROOT / "dbt_project"
FIXTURES_DIR = REPO_ROOT / "fixtures"

_DBT_BIN = str(Path(sys.executable).parent / "dbt")


@dataclasses.dataclass
class IsolatedLake:
    """Filesystem layout for one test's private lake + warehouse."""

    duckdb_path: Path
    lake_dir: Path
    catalog_db: Path
    manifest_path: Path

    def land(self, chaos_mode: str = "none"):
        from ingestion.land_raw_data import land

        return land(
            chaos_mode=chaos_mode,
            duckdb_path=self.duckdb_path,
            lake_dir=self.lake_dir,
            catalog_db=self.catalog_db,
            manifest_path=self.manifest_path,
        )

    def dbt(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        """Run a dbt command against this fixture's isolated DuckDB warehouse."""
        cmd = [_DBT_BIN, *args, "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)]
        env = {**__import__("os").environ, "DUCKDB_PATH": str(self.duckdb_path)}
        result = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True)
        if check and result.returncode != 0:
            raise AssertionError(
                f"dbt {' '.join(args)} failed (exit {result.returncode}):\n{result.stdout}\n{result.stderr}"
            )
        return result


@pytest.fixture
def isolated_lake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> IsolatedLake:
    """A fresh, isolated lake/warehouse for a single test. Sets DUCKDB_PATH too."""
    duckdb_path = tmp_path / "warehouse.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(duckdb_path))
    return IsolatedLake(
        duckdb_path=duckdb_path,
        lake_dir=tmp_path / "lake",
        catalog_db=tmp_path / "iceberg_catalog.db",
        manifest_path=tmp_path / "manifests" / "ingest_manifest.json",
    )


@pytest.fixture
def clean_staged_lake(isolated_lake: IsolatedLake) -> IsolatedLake:
    """An isolated lake that has already been landed (chaos_mode=none) and dbt-built through intermediate."""
    isolated_lake.land(chaos_mode="none")
    isolated_lake.dbt("run", "--select", "staging intermediate")
    return isolated_lake
