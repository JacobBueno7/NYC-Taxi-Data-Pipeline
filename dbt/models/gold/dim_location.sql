select
    l.location_hk,
    l.location_id,
    coalesce(s.borough, 'Unknown') as borough,
    coalesce(s.zone, 'Unknown') as zone,
    coalesce(s.service_zone, 'Unknown') as service_zone
from {{ ref('hub_location') }} l
left join {{ ref('sat_location_details') }} s on l.location_hk = s.location_hk
