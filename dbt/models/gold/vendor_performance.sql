select
    v.vendor_name,
    count(*) as total_trips,
    round(sum(f.total_amount), 2) as total_revenue,
    round(avg(f.trip_distance), 2) as avg_distance,
    round(avg(case when f.fare_amount > 0 then f.tip_amount / f.fare_amount * 100 else null end), 2) as avg_tip_pct
from {{ ref('fct_trips') }} f
inner join {{ ref('dim_vendor') }} v on f.vendor_hk = v.vendor_hk
group by v.vendor_name
order by total_trips desc
