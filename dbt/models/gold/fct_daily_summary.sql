select
    pickup_date as trip_date,
    count(*) as total_trips,
    round(avg(trip_distance), 2) as avg_distance,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(tip_amount), 2) as avg_tip,
    round(sum(tip_amount), 2) as total_tip,
    round(sum(total_amount), 2) as total_revenue
from {{ ref('fct_trips') }}
group by 1
order by 1
