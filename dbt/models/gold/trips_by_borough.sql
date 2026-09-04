with trips as (
    select
        f.trip_hk,
        f.total_amount,
        f.trip_distance,
        pu.borough as pickup_borough,
        pu.zone as pickup_zone,
        do.borough as dropoff_borough,
        do.zone as dropoff_zone
    from {{ ref('fct_trips') }} f
    left join {{ ref('dim_location') }} pu on f.pickup_location_hk = pu.location_hk
    left join {{ ref('dim_location') }} do on f.dropoff_location_hk = do.location_hk
)

select
    pickup_borough,
    dropoff_borough,
    count(*) as total_trips,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(trip_distance), 2) as avg_distance
from trips
group by pickup_borough, dropoff_borough
order by total_trips desc
