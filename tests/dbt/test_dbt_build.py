"""
dbt macro/model behavior on DuckDB.

Covers: staging casts the raw source correctly, the intermediate layer's
`safe_divide`/`trip_duration_minutes` macros (also pinned by dbt's own unit
tests in models/intermediate/_intermediate__models.yml), marts build and
pass their data tests, and -- critically -- that `dbt build --select marts`
on top of a clean intermediate layer produces the row counts/relationships
we expect.
"""

from __future__ import annotations


def test_staging_casts_types_correctly(clean_staged_lake):
    import duckdb

    con = duckdb.connect(str(clean_staged_lake.duckdb_path), read_only=True)
    try:
        row = con.execute("select * from main_staging.stg_trips limit 1").fetchone()
        columns = [d[0] for d in con.description]
        count = con.execute("select count(*) from main_staging.stg_trips").fetchone()[0]
    finally:
        con.close()

    assert count == 2000
    assert row is not None
    expected_columns = {
        "trip_id",
        "vendor_id",
        "pickup_at",
        "dropoff_at",
        "passenger_count",
        "trip_distance_miles",
        "pickup_location_id",
        "dropoff_location_id",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type",
    }
    assert expected_columns <= set(columns)


def test_intermediate_derives_duration_and_fare_per_mile(clean_staged_lake):
    import duckdb

    con = duckdb.connect(str(clean_staged_lake.duckdb_path), read_only=True)
    try:
        df = con.execute(
            "select trip_duration_minutes, fare_per_mile, trip_distance_miles, fare_amount "
            "from main_intermediate.int_trips_enriched"
        ).fetchdf()
    finally:
        con.close()

    assert len(df) == 2000
    assert (df["trip_duration_minutes"] > 0).all()

    # fare_per_mile should exactly equal fare_amount / trip_distance_miles for
    # rows with nonzero distance (pins down the safe_divide macro end to end,
    # on top of dbt's own unit test for the same macro).
    nonzero = df[df["trip_distance_miles"] != 0]
    computed = nonzero["fare_amount"] / nonzero["trip_distance_miles"]
    assert (abs(nonzero["fare_per_mile"] - computed) < 1e-9).all()


def test_dbt_unit_tests_pass(clean_staged_lake):
    """Runs dbt's own unit tests (safe_divide / trip_duration_minutes edge cases)."""
    result = clean_staged_lake.dbt("test", "--select", "test_type:unit")
    assert "PASS=2" in result.stdout or "PASS=2" in result.stderr


def test_marts_build_and_pass_all_data_tests(clean_staged_lake):
    result = clean_staged_lake.dbt("build", "--select", "marts")
    combined = result.stdout + result.stderr
    assert "ERROR" not in combined.upper().replace("0 ERROR", "")
    assert "Completed successfully" in combined

    import duckdb

    con = duckdb.connect(str(clean_staged_lake.duckdb_path), read_only=True)
    try:
        fct_count = con.execute("select count(*) from main_marts.fct_trips").fetchone()[0]
        dim_count = con.execute("select count(*) from main_marts.dim_zones").fetchone()[0]
        orphans = con.execute(
            "select count(*) from main_marts.fct_trips f "
            "left join main_marts.dim_zones z on f.pickup_location_id = z.location_id "
            "where z.location_id is null"
        ).fetchone()[0]
    finally:
        con.close()

    assert fct_count == 2000
    assert dim_count == 15
    assert orphans == 0
