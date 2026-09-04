select
    LocationID as location_id,
    Borough as borough,
    Zone as zone,
    service_zone,
    current_timestamp as _loaded_at,
    'taxi_zone_lookup_csv' as _record_source
from {{ source('raw', 'taxi_zone_lookup') }}
