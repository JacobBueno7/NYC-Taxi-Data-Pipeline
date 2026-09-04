with spine as (
    select unnest(generate_series(date '2020-01-01', date '2026-12-31', interval 1 day)) as date_day
)

select
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    strftime(date_day, '%B') as month_name,
    extract(day from date_day) as day_of_month,
    extract(dow from date_day) as day_of_week,
    strftime(date_day, '%A') as day_name,
    case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend
from spine
