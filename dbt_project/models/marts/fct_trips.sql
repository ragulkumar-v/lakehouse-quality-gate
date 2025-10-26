-- Mart layer: the table dashboards/BI actually query. Only ever built after
-- the Great Expectations gate has passed the intermediate layer -- see
-- dags/lakehouse_quality_gate_dag.py.

select
    trip_id,
    vendor_id,
    pickup_at,
    dropoff_at,
    passenger_count,
    trip_distance_miles,
    fare_amount,
    tip_amount,
    total_amount,
    payment_type,
    pickup_location_id,
    dropoff_location_id,
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    trip_duration_minutes,
    fare_per_mile
from {{ ref('int_trips_enriched') }}
