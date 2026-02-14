"""
Generates the bundled offline sample of "raw public data" used by this project.

In a real deployment, `ingestion/land_raw_data.py` pulls a slice of the NYC TLC
Yellow Taxi Trip Records (a genuinely public, no-auth-required dataset published
by NYC at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) straight
into the lake. Because this repository must be buildable and testable fully
offline, we ship a small synthetic sample with the *same shape/dtypes* as that
public dataset under fixtures/. This script is what generated it, committed for
reproducibility -- re-run it any time to regenerate a fresh sample.

Usage:
    python fixtures/generate_fixture.py
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent

ZONES = [
    (1, "Manhattan", "Battery Park"),
    (2, "Manhattan", "Chelsea"),
    (3, "Manhattan", "Midtown East"),
    (4, "Brooklyn", "Williamsburg"),
    (5, "Brooklyn", "Park Slope"),
    (6, "Queens", "Astoria"),
    (7, "Queens", "LaGuardia Airport"),
    (8, "Bronx", "Fordham"),
    (9, "Staten Island", "St. George"),
    (10, "Manhattan", "Financial District"),
    (11, "Manhattan", "Harlem"),
    (12, "Brooklyn", "DUMBO"),
    (13, "Queens", "JFK Airport"),
    (14, "Bronx", "Riverdale"),
    (15, "Manhattan", "Upper West Side"),
]

PAYMENT_TYPES = [1, 1, 1, 2, 2, 3, 4]  # weighted toward credit card (1) / cash (2)
VENDOR_IDS = [1, 2]


def _write_zone_lookup() -> None:
    path = FIXTURES_DIR / "zone_lookup.csv"
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["location_id", "borough", "zone"])
        writer.writerows(ZONES)


def _write_trips_sample(n_rows: int = 2000, seed: int = 42) -> None:
    rng = random.Random(seed)
    path = FIXTURES_DIR / "public_trips_sample.csv"
    start = datetime(2025, 6, 1, 0, 0, 0)

    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "trip_id",
                "vendor_id",
                "pickup_datetime",
                "dropoff_datetime",
                "passenger_count",
                "trip_distance",
                "pickup_location_id",
                "dropoff_location_id",
                "fare_amount",
                "tip_amount",
                "total_amount",
                "payment_type",
            ]
        )
        for i in range(1, n_rows + 1):
            pickup = start + timedelta(minutes=rng.randint(0, 30 * 24 * 60))
            duration_min = rng.randint(2, 55)
            dropoff = pickup + timedelta(minutes=duration_min)
            distance = round(max(0.2, rng.gauss(3.2, 2.1)), 2)
            fare = round(max(3.0, 2.5 + distance * rng.uniform(2.2, 3.4)), 2)
            tip = round(fare * rng.choice([0, 0, 0.1, 0.15, 0.2]), 2)
            total = round(fare + tip + 0.3, 2)
            pickup_zone = rng.choice(ZONES)[0]
            dropoff_zone = rng.choice(ZONES)[0]

            writer.writerow(
                [
                    i,
                    rng.choice(VENDOR_IDS),
                    pickup.isoformat(sep=" "),
                    dropoff.isoformat(sep=" "),
                    rng.choice([1, 1, 1, 2, 3, 4]),
                    distance,
                    pickup_zone,
                    dropoff_zone,
                    fare,
                    tip,
                    total,
                    rng.choice(PAYMENT_TYPES),
                ]
            )


if __name__ == "__main__":
    _write_zone_lookup()
    _write_trips_sample()
    print(f"Wrote fixtures to {FIXTURES_DIR}")
