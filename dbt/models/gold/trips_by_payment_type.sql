with mapped as (
    select
        case payment_type
            when 1 then 'Credit Card'
            when 2 then 'Cash'
            when 3 then 'No Charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            when 6 then 'Voided Trip'
            else 'Unknown'
        end as payment_type_desc,
        total_amount,
        tip_amount,
        fare_amount
    from {{ ref('fct_trips') }}
)

select
    payment_type_desc,
    count(*) as total_trips,
    round(sum(total_amount), 2) as total_revenue,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(tip_amount), 2) as avg_tip,
    round(avg(case when fare_amount > 0 then tip_amount / fare_amount * 100 else null end), 2) as avg_tip_pct
from mapped
group by payment_type_desc
order by total_trips desc
