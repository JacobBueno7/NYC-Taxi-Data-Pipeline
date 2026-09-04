select
    v.vendor_hk,
    v.vendor_id,
    coalesce(s.vendor_name, 'Unknown Vendor') as vendor_name
from {{ ref('hub_vendor') }} v
left join {{ ref('sat_vendor_details') }} s on v.vendor_hk = s.vendor_hk
