select
    location_id,
    borough,
    zone
from {{ ref('stg_zones') }}
