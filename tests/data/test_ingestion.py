"""Unit tests for ingestion/land_raw_data.py -- real Iceberg + DuckDB writes, fully offline."""

from __future__ import annotations

import duckdb
import pytest

from ingestion.land_raw_data import land


def test_land_none_mode_writes_expected_row_counts(isolated_lake):
    manifest = isolated_lake.land(chaos_mode="none")

    assert manifest.chaos_mode == "none"
    assert manifest.trips_rows == 2000
    assert manifest.zones_rows == 15
    assert manifest.trips_snapshot_id is not None
    assert manifest.zones_snapshot_id is not None
    assert isolated_lake.manifest_path.exists()

    con = duckdb.connect(str(isolated_lake.duckdb_path), read_only=True)
    try:
        trip_count = con.execute("select count(*) from raw.trips").fetchone()[0]
        zone_count = con.execute("select count(*) from raw.zones").fetchone()[0]
    finally:
        con.close()
    assert trip_count == 2000
    assert zone_count == 15


def test_land_writes_real_iceberg_metadata(isolated_lake):
    """The lake directory should contain genuine Iceberg metadata/data files, not a stub."""
    isolated_lake.land(chaos_mode="none")

    trips_metadata = isolated_lake.lake_dir / "raw" / "trips" / "metadata"
    trips_data = isolated_lake.lake_dir / "raw" / "trips" / "data"
    assert trips_metadata.is_dir()
    assert trips_data.is_dir()
    assert any(p.suffix == ".json" for p in trips_metadata.iterdir())
    assert any(p.suffix == ".parquet" for p in trips_data.iterdir())
    assert isolated_lake.catalog_db.exists()


def test_land_schema_drift_renames_column_in_the_raw_table(isolated_lake):
    isolated_lake.land(chaos_mode="schema_drift")

    con = duckdb.connect(str(isolated_lake.duckdb_path), read_only=True)
    try:
        columns = {row[0] for row in con.execute("describe raw.trips").fetchall()}
    finally:
        con.close()

    assert "trip_distance" not in columns
    assert "trip_distance_mi" in columns


def test_land_dup_pk_produces_more_rows_than_the_fixture(isolated_lake):
    manifest = isolated_lake.land(chaos_mode="dup_pk")
    assert manifest.trips_rows > 2000


def test_land_is_a_full_refresh_not_an_accumulation(isolated_lake):
    """Landing twice should not double the row count -- it's drop-and-recreate, not append."""
    isolated_lake.land(chaos_mode="none")
    manifest_second = isolated_lake.land(chaos_mode="none")
    assert manifest_second.trips_rows == 2000


def test_land_rejects_unknown_chaos_mode(isolated_lake):
    with pytest.raises(ValueError, match="Unknown chaos_mode"):
        isolated_lake.land(chaos_mode="totally_made_up")


def test_land_default_paths_are_independent_of_import_order(tmp_path, monkeypatch):
    """
    Regression guard: `land()` must resolve DUCKDB_PATH fresh from the
    environment at call time, not freeze whatever it was when
    `ingestion.land_raw_data` first got imported into the process.
    """
    duckdb_path = tmp_path / "freshly_set.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(duckdb_path))

    land(
        chaos_mode="none",
        lake_dir=tmp_path / "lake",
        catalog_db=tmp_path / "catalog.db",
        manifest_path=tmp_path / "manifest.json",
    )

    assert duckdb_path.exists(), (
        "land() should have written to the DUCKDB_PATH set just now, not a stale default"
    )
