"""
Chaos injection: deliberately corrupts the raw dataset in ways real upstream
producers actually break pipelines, so we can prove the Great Expectations
gate (quality/checkpoint.py) catches each one before it reaches the marts.

Modes:
    none            -- pass the data through untouched (control group)
    null_flood      -- floods a critical column (fare_amount) with nulls,
                       simulating an upstream extraction bug
    schema_drift    -- silently renames a column, simulating an upstream
                       contract change nobody announced
    dup_pk          -- duplicates primary-key rows, simulating a retried
                       extraction job double-writing records
    negative_values -- introduces impossible negative fares/distances,
                       simulating a sign-flip bug in the source system

Used both as a library (`apply_chaos`) by ingestion/land_raw_data.py's
`--chaos-mode` flag, and as a standalone CLI for local experimentation:

    python -m chaos.inject_chaos --mode null_flood --in fixtures/public_trips_sample.csv --out /tmp/corrupted.csv
"""

from __future__ import annotations

import argparse
import random

import pyarrow as pa
import pyarrow.csv as pacsv

CHAOS_MODES = {"none", "null_flood", "schema_drift", "dup_pk", "negative_values"}


def _null_flood(
    table: pa.Table, column: str = "fare_amount", fraction: float = 0.35, seed: int = 7
) -> pa.Table:
    """Overwrite `fraction` of a critical column with nulls."""
    rng = random.Random(seed)
    col = table.column(column).to_pylist()
    n_to_null = int(len(col) * fraction)
    indices = set(rng.sample(range(len(col)), n_to_null))
    new_col = [None if i in indices else v for i, v in enumerate(col)]
    idx = table.schema.get_field_index(column)
    return table.set_column(idx, column, pa.array(new_col, type=table.schema.field(column).type))


def _schema_drift(
    table: pa.Table, old_name: str = "trip_distance", new_name: str = "trip_distance_mi"
) -> pa.Table:
    """Rename a column the staging model expects, simulating a breaking upstream change."""
    names = [new_name if n == old_name else n for n in table.schema.names]
    return table.rename_columns(names)


def _dup_pk(table: pa.Table, fraction: float = 0.1, seed: int = 11) -> pa.Table:
    """Duplicate a slice of rows to violate the primary-key uniqueness expectation."""
    rng = random.Random(seed)
    n = table.num_rows
    n_dupes = max(1, int(n * fraction))
    dup_indices = rng.sample(range(n), min(n_dupes, n))
    dupes = table.take(pa.array(dup_indices))
    return pa.concat_tables([table, dupes])


def _negative_values(table: pa.Table, fraction: float = 0.15, seed: int = 13) -> pa.Table:
    """Flip the sign of fare_amount / trip_distance on a slice of rows."""
    rng = random.Random(seed)
    n = table.num_rows
    n_bad = int(n * fraction)
    indices = set(rng.sample(range(n), n_bad))

    for column in ("fare_amount", "trip_distance"):
        col = table.column(column).to_pylist()
        new_col = [(-abs(v) if (i in indices and v is not None) else v) for i, v in enumerate(col)]
        idx = table.schema.get_field_index(column)
        table = table.set_column(idx, column, pa.array(new_col, type=table.schema.field(column).type))
    return table


_HANDLERS = {
    "none": lambda t: t,
    "null_flood": _null_flood,
    "schema_drift": _schema_drift,
    "dup_pk": _dup_pk,
    "negative_values": _negative_values,
}


def apply_chaos(table: pa.Table, mode: str) -> pa.Table:
    if mode not in CHAOS_MODES:
        raise ValueError(f"Unknown chaos mode {mode!r}; choose from {sorted(CHAOS_MODES)}")
    return _HANDLERS[mode](table)


def _cli() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=sorted(CHAOS_MODES))
    parser.add_argument("--in", dest="in_path", required=True)
    parser.add_argument("--out", dest="out_path", required=True)
    args = parser.parse_args()

    table = pacsv.read_csv(args.in_path)
    corrupted = apply_chaos(table, args.mode)
    pacsv.write_csv(corrupted, args.out_path)
    print(f"Wrote {corrupted.num_rows} rows ({args.mode}) to {args.out_path}")


if __name__ == "__main__":
    _cli()
