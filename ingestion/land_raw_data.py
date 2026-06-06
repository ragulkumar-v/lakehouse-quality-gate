"""
Raw ingestion: lands public trip data as real Apache Iceberg tables and makes
the current snapshot available to dbt as DuckDB "raw" tables.

Why Iceberg *and* DuckDB tables, rather than just one or the other?

  * Iceberg (via pyiceberg, a SQLite catalog + a local/MinIO warehouse) is the
    system of record for the raw zone: it gives us schema evolution, snapshot
    history and time travel on the landed data, exactly like a production
    lakehouse would use on top of MinIO/S3. This is real, working Iceberg --
    not a stub -- verifiable fully offline (SqlCatalog needs no cluster).

  * dbt-duckdb reads its sources from a plain DuckDB database file. DuckDB can
    read Iceberg tables directly via its `iceberg` extension, but that
    extension is fetched over the network on first LOAD, which would violate
    this project's "no network at test time" requirement. So after writing
    the Iceberg snapshot, we materialize it straight into the DuckDB warehouse
    dbt points at. In a real cluster deployment you would instead point
    dbt-duckdb's iceberg scanner (or Trino/Spark) directly at the Iceberg
    tables -- see README "Design notes" for the swap.

Usage:
    python -m ingestion.land_raw_data --chaos-mode none
    python -m ingestion.land_raw_data --chaos-mode null_flood
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog

from chaos.inject_chaos import CHAOS_MODES, apply_chaos
from ingestion.storage_backend import get_storage_backend

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = REPO_ROOT / "fixtures"
DATA_DIR = REPO_ROOT / "data"
LAKE_DIR = DATA_DIR / "lake"
CATALOG_DB = DATA_DIR / "iceberg_catalog.db"
MANIFEST_PATH = DATA_DIR / "manifests" / "ingest_manifest.json"


def _default_duckdb_path() -> Path:
    """
    Resolved fresh on every call (NOT a module-level constant): this module
    is imported once and cached by Python, so a constant computed at import
    time would freeze whatever `DUCKDB_PATH` happened to be at that moment --
    silently ignoring any later `os.environ["DUCKDB_PATH"] = ...` a caller
    (or a test's `monkeypatch.setenv`) makes afterwards.
    """
    return Path(os.environ.get("DUCKDB_PATH", DATA_DIR / "warehouse.duckdb"))


NAMESPACE = "raw"


@dataclass
class IngestManifest:
    ingested_at: str
    chaos_mode: str
    trips_rows: int
    zones_rows: int
    trips_snapshot_id: int | None
    zones_snapshot_id: int | None
    warehouse_uri: str


def _get_catalog(lake_dir: Path, catalog_db: Path) -> Catalog:
    catalog_db.parent.mkdir(parents=True, exist_ok=True)
    backend = get_storage_backend(lake_dir)
    warehouse_uri = backend.warehouse_uri()
    if not warehouse_uri.startswith(("s3://", "file://")):
        warehouse_uri = f"file://{warehouse_uri}"
    catalog = SqlCatalog(
        "lakehouse",
        **{"uri": f"sqlite:///{catalog_db}", "warehouse": warehouse_uri},
    )
    catalog.create_namespace_if_not_exists(NAMESPACE)
    return catalog


def _land_table(catalog: Catalog, table_name: str, arrow_table: pa.Table):
    """
    Full-refresh land `arrow_table` as an Iceberg table.

    This is deliberately a drop-and-recreate rather than an in-place
    `overwrite()`: pyiceberg enforces schema compatibility on overwrite, which
    would reject an upstream schema change (e.g. a renamed column) at the
    Iceberg layer itself. That's a fine outcome for a production incremental
    pipeline, but for this project the point is to let a silent schema change
    land -- exactly like a naive/real ingestion job would -- so the
    Great Expectations gate between staging and marts is what catches it
    (see quality/checkpoint.py and the "schema_drift" chaos mode). Real
    incremental ingestion would instead call `table.update_schema()` to
    evolve the schema deliberately.
    """
    full_name = f"{NAMESPACE}.{table_name}"
    if catalog.table_exists(full_name):
        catalog.drop_table(full_name)
    table = catalog.create_table(full_name, schema=arrow_table.schema)
    table.append(arrow_table)
    table.refresh()
    return table


def _materialize_into_duckdb(con: duckdb.DuckDBPyConnection, table_name: str, table) -> int:
    arrow_data = table.scan().to_arrow()
    con.register("_incoming", arrow_data)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {NAMESPACE}")
    con.execute(f"CREATE OR REPLACE TABLE {NAMESPACE}.{table_name} AS SELECT * FROM _incoming")
    con.unregister("_incoming")
    return arrow_data.num_rows


def land(
    chaos_mode: str = "none",
    source_dir: Path | None = None,
    duckdb_path: Path | None = None,
    lake_dir: Path | None = None,
    catalog_db: Path | None = None,
    manifest_path: Path | None = None,
) -> IngestManifest:
    """
    Land raw source data (optionally chaos-corrupted) as Iceberg tables and
    materialize the current snapshot into DuckDB for dbt.

    All paths default to this repo's `data/` directory (and `DUCKDB_PATH`
    env var for the warehouse) so the CLI and Airflow tasks need zero
    configuration. Every path is independently overridable so tests can land
    fully-isolated data into a `tmp_path` without disturbing -- or being
    disturbed by -- the shared local `data/` directory (see tests/conftest.py).
    """
    if chaos_mode not in CHAOS_MODES:
        raise ValueError(f"Unknown chaos_mode {chaos_mode!r}; choose from {sorted(CHAOS_MODES)}")

    source_dir = source_dir or FIXTURES_DIR
    duckdb_path = duckdb_path or _default_duckdb_path()
    lake_dir = lake_dir or LAKE_DIR
    catalog_db = catalog_db or CATALOG_DB
    manifest_path = manifest_path or MANIFEST_PATH

    trips = pacsv.read_csv(source_dir / "public_trips_sample.csv")
    zones = pacsv.read_csv(source_dir / "zone_lookup.csv")

    if chaos_mode != "none":
        trips = apply_chaos(trips, chaos_mode)

    catalog = _get_catalog(lake_dir, catalog_db)
    trips_table = _land_table(catalog, "trips", trips)
    zones_table = _land_table(catalog, "zones", zones)

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(duckdb_path))
    try:
        trips_rows = _materialize_into_duckdb(con, "trips", trips_table)
        zones_rows = _materialize_into_duckdb(con, "zones", zones_table)
    finally:
        con.close()

    manifest = IngestManifest(
        ingested_at=datetime.now(UTC).isoformat(),
        chaos_mode=chaos_mode,
        trips_rows=trips_rows,
        zones_rows=zones_rows,
        trips_snapshot_id=trips_table.current_snapshot().snapshot_id
        if trips_table.current_snapshot()
        else None,
        zones_snapshot_id=zones_table.current_snapshot().snapshot_id
        if zones_table.current_snapshot()
        else None,
        warehouse_uri=catalog.properties.get("warehouse", ""),
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(asdict(manifest), indent=2))
    return manifest


def _cli() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chaos-mode", default="none", choices=sorted(CHAOS_MODES))
    args = parser.parse_args()
    manifest = land(chaos_mode=args.chaos_mode)
    print(json.dumps(asdict(manifest), indent=2))


if __name__ == "__main__":
    _cli()
