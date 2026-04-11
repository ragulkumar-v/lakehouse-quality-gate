"""Unit tests for chaos/inject_chaos.py -- pure pyarrow transforms, no I/O."""

from __future__ import annotations

import pyarrow as pa
import pytest

from chaos.inject_chaos import CHAOS_MODES, apply_chaos


def _sample_table(n: int = 100) -> pa.Table:
    return pa.table(
        {
            "trip_id": list(range(1, n + 1)),
            "fare_amount": [10.0 + i for i in range(n)],
            "trip_distance": [1.0 + 0.1 * i for i in range(n)],
        }
    )


def test_none_mode_passes_through_unchanged():
    table = _sample_table()
    result = apply_chaos(table, "none")
    assert result.equals(table)


def test_null_flood_nulls_roughly_the_configured_fraction():
    table = _sample_table(200)
    result = apply_chaos(table, "null_flood")
    null_count = result.column("fare_amount").null_count
    # _null_flood defaults to fraction=0.35 of the column.
    assert null_count == 70
    # Other columns are untouched.
    assert result.column("trip_id").null_count == 0


def test_null_flood_is_deterministic():
    table = _sample_table(200)
    a = apply_chaos(table, "null_flood")
    b = apply_chaos(table, "null_flood")
    assert a.column("fare_amount").to_pylist() == b.column("fare_amount").to_pylist()


def test_schema_drift_renames_trip_distance():
    table = _sample_table()
    result = apply_chaos(table, "schema_drift")
    assert "trip_distance" not in result.schema.names
    assert "trip_distance_mi" in result.schema.names
    # Row count and values are otherwise untouched -- this is purely a rename.
    assert result.num_rows == table.num_rows
    assert result.column("trip_distance_mi").to_pylist() == table.column("trip_distance").to_pylist()


def test_dup_pk_increases_row_count_and_creates_duplicate_ids():
    table = _sample_table(100)
    result = apply_chaos(table, "dup_pk")
    assert result.num_rows > table.num_rows
    ids = result.column("trip_id").to_pylist()
    assert len(ids) != len(set(ids)), "dup_pk should introduce duplicate primary keys"


def test_negative_values_flips_sign_on_a_slice_only():
    table = _sample_table(200)
    result = apply_chaos(table, "negative_values")
    fares = result.column("fare_amount").to_pylist()
    negative_count = sum(1 for f in fares if f < 0)
    assert negative_count > 0
    # fraction=0.15 of 200 rows == 30
    assert negative_count == 30
    # Untouched rows keep their original positive values.
    original = table.column("fare_amount").to_pylist()
    for i, (orig, new) in enumerate(zip(original, fares, strict=True)):
        assert abs(new) == pytest.approx(orig), f"row {i} magnitude changed unexpectedly"


def test_unknown_mode_raises():
    with pytest.raises(ValueError, match="Unknown chaos mode"):
        apply_chaos(_sample_table(), "not_a_real_mode")


def test_all_declared_modes_are_handled():
    """Every mode in CHAOS_MODES must actually be applyable without error."""
    table = _sample_table(50)
    for mode in CHAOS_MODES:
        result = apply_chaos(table, mode)
        assert result.num_rows >= table.num_rows
