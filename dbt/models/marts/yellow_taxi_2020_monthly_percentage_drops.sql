with monthly as (
    select month(trip_date) as month_num, strftime(trip_date, '%B') as month, sum(total_trips) as total_trips, sum(total_revenue) as total_revenue
    from {{ ref('fct_yellow_taxi') }}
    where year(trip_date) = 2020
    group by month(trip_date), strftime(trip_date, '%B')
),
monthly_with_prev as (
    select *, lag(total_trips) over (order by month_num) as prev_trips, lag(total_revenue) over (order by month_num) as prev_revenue
    from monthly
),
final as (
    select month, round((1 - total_trips/prev_trips)*100, 2) || '%' as trip_drop_percent, round((1 - total_revenue/prev_revenue)*100, 2) || '%' as revenue_drop_percent
    from monthly_with_prev
)
select * from final