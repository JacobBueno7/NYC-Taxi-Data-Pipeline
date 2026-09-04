select
    d.day_name,
    d.day_of_week,
    extract(hour from f.pickup_datetime) as pickup_hour,
    count(*) as total_trips,
    round(avg(f.fare_amount), 2) as avg_fare,
    round(avg(case when f.fare_amount > 0 then f.tip_amount / f.fare_amount * 100 else null end), 2) as avg_tip_pct
from {{ ref('fct_trips') }} f
inner join {{ ref('dim_date') }} d on f.pickup_date = d.date_day
group by d.day_name, d.day_of_week, extract(hour from f.pickup_datetime)
order by d.day_of_week, pickup_hour
