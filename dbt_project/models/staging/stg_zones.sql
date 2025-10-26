with source as (
    select * from {{ source('raw', 'zones') }}
),

renamed as (
    select
        cast(location_id as integer) as location_id,
        cast(borough as varchar)     as borough,
        cast(zone as varchar)        as zone
    from source
)

select * from renamed
