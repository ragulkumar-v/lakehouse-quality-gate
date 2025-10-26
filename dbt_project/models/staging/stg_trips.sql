-- Staging layer: 1:1 with the raw source, light typing/renaming only.
-- No business logic lives here -- that starts in intermediate/.

with source as (
    select * from {{ source('raw', 'trips') }}
),

renamed as (
    select
        cast(trip_id as bigint)               as trip_id,
        cast(vendor_id as integer)             as vendor_id,
        cast(pickup_datetime as timestamp)     as pickup_at,
        cast(dropoff_datetime as timestamp)    as dropoff_at,
        cast(passenger_count as integer)       as passenger_count,
        cast(trip_distance as double)          as trip_distance_miles,
        cast(pickup_location_id as integer)    as pickup_location_id,
        cast(dropoff_location_id as integer)   as dropoff_location_id,
        cast(fare_amount as double)            as fare_amount,
        cast(tip_amount as double)             as tip_amount,
        cast(total_amount as double)           as total_amount,
        cast(payment_type as integer)          as payment_type
    from source
)

select * from renamed
