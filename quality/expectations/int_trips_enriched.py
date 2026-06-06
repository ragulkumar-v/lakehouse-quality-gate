"""
Expectation suite for `intermediate.int_trips_enriched` -- the exact model
the quality gate (quality/checkpoint.py) validates before marts are allowed
to build.

Defined in Python rather than a checked-in JSON suite file: Great
Expectations' JSON suite schema has changed across major versions (this
project pins 1.19.x), and a Python function that builds an `ExpectationSuite`
from the current `great_expectations.expectations` API is far less likely to
silently bit-rot than a hand-authored JSON blob. It is still, in every sense
that matters, "the expectations" -- just expressed as code instead of data.

Each expectation below maps to a specific real-world failure mode a
lakehouse ingestion job can introduce (see chaos/inject_chaos.py, which
manufactures exactly these failures so the gate can be proven against them):

  * trip_id not_null / unique          -> catches `dup_pk` (retried writer
                                           double-inserts records)
  * fare_amount not_null (mostly=0.98) -> catches `null_flood` (upstream
                                           extraction bug nulls out a
                                           critical revenue column)
  * fare_amount / trip_distance_miles
    between 0 and a sane ceiling       -> catches `negative_values` (a
                                           sign-flip bug in the source
                                           system)
  * passenger_count between 0 and 8    -> catches generally malformed rows
                                           that dup/null/sign-flip chaos can
                                           produce as a side effect
  * trip_duration_minutes not_null     -> catches broken joins/derivations,
                                           independent of which raw column
                                           chaos touched
"""

from __future__ import annotations

import great_expectations as gx

SUITE_NAME = "int_trips_enriched_suite"

# Fraction of a column that is allowed to be null/violate a rule before the
# expectation itself is considered "successful". Kept intentionally tight --
# this is a *gate*, not a dashboard -- but non-zero so that a handful of
# legitimately missing values (e.g. a cash trip with no tip metadata) don't
# trip the whole pipeline.
NOT_NULL_MOSTLY = 0.98


def build_suite() -> gx.ExpectationSuite:
    """Construct the (unsaved) ExpectationSuite for int_trips_enriched."""
    suite = gx.ExpectationSuite(name=SUITE_NAME)

    expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="trip_id",
            meta={"notes": "Every enriched trip must carry its source primary key."},
        ),
        gx.expectations.ExpectColumnValuesToBeUnique(
            column="trip_id",
            meta={"notes": "Catches duplicate-write / retried-extraction bugs (chaos: dup_pk)."},
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="fare_amount",
            mostly=NOT_NULL_MOSTLY,
            meta={"notes": "Catches a critical revenue column being null-flooded (chaos: null_flood)."},
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="fare_amount",
            min_value=0,
            max_value=1000,
            mostly=NOT_NULL_MOSTLY,
            meta={"notes": "Catches sign-flip / impossible-value bugs (chaos: negative_values)."},
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="trip_distance_miles",
            min_value=0,
            max_value=500,
            mostly=NOT_NULL_MOSTLY,
            meta={"notes": "Catches sign-flip / impossible-value bugs (chaos: negative_values)."},
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="passenger_count",
            min_value=0,
            max_value=8,
            mostly=NOT_NULL_MOSTLY,
            meta={"notes": "Sanity bound on passenger counts; guards against generally malformed rows."},
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="trip_duration_minutes",
            mostly=NOT_NULL_MOSTLY,
            meta={"notes": "Derived column; a flood of nulls here means the join/derivation itself broke."},
        ),
    ]
    for expectation in expectations:
        suite.add_expectation(expectation)
    return suite
