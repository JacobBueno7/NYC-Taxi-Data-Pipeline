SELECT year(trip_date) as year, sum(total_trips) as total_trips, sum(total_revenue) as total_revenue
FROM {{ ref('fct_yellow_taxi') }}
where year(trip_date) <= 2026
group by year(trip_date)