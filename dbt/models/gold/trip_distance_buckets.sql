with bucketed as (
    select
        case
            when trip_distance < 2 then '0-2 mi (short)'
            when trip_distance < 5 then '2-5 mi (medium)'
            when trip_distance < 10 then '5-10 mi (long)'
            else '10+ mi (very long)'
        end as distance_bucket,
        trip_distance,
        fare_amount,
        tip_amount,
        trip_duration_minutes
    from {{ ref('fct_trips') }}
)

select
    distance_bucket,
    count(*) as total_trips,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(trip_duration_minutes), 2) as avg_duration_minutes,
    round(avg(case when fare_amount > 0 then tip_amount / fare_amount * 100 else null end), 2) as avg_tip_pct
from bucketed
group by distance_bucket
order by min(trip_distance)
