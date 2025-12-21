-- Intermediate layer: joins staging models and derives the business logic
-- (duration, fare-per-mile) that the marts + Great Expectations gate rely on.

with trips as (
    select * from {{ ref('stg_trips') }}
),

pickup_zones as (
    select location_id, borough as pickup_borough, zone as pickup_zone
    from {{ ref('stg_zones') }}
),

dropoff_zones as (
    select location_id, borough as dropoff_borough, zone as dropoff_zone
    from {{ ref('stg_zones') }}
),

enriched as (
    select
        trips.trip_id,
        trips.vendor_id,
        trips.pickup_at,
        trips.dropoff_at,
        trips.passenger_count,
        trips.trip_distance_miles,
        trips.fare_amount,
        trips.tip_amount,
        trips.total_amount,
        trips.payment_type,
        trips.pickup_location_id,
        trips.dropoff_location_id,
        pickup_zones.pickup_borough,
        pickup_zones.pickup_zone,
        dropoff_zones.dropoff_borough,
        dropoff_zones.dropoff_zone,
        {{ trip_duration_minutes('trips.pickup_at', 'trips.dropoff_at') }} as trip_duration_minutes,
        {{ safe_divide('trips.fare_amount', 'trips.trip_distance_miles') }} as fare_per_mile
    from trips
    left join pickup_zones on trips.pickup_location_id = pickup_zones.location_id
    left join dropoff_zones on trips.dropoff_location_id = dropoff_zones.location_id
)

select * from enriched
