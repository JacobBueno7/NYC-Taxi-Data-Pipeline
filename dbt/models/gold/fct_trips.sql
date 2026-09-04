with trip_facts as (
    select
        l.trip_hk,
        l.vendor_hk,
        l.pickup_location_hk,
        l.dropoff_location_hk,
        s.pickup_datetime,
        s.dropoff_datetime,
        s.passenger_cnt,
        s.trip_distance,
        s.ratecode_id,
        s.payment_type,
        s.fare_amount,
        s.extra,
        s.mta_tax,
        s.tip_amount,
        s.tolls_amount,
        s.improvement_surcharge,
        s.congestion_surcharge,
        s.airport_fee,
        s.total_amount
    from {{ ref('link_trip') }} l
    inner join {{ ref('sat_trip_details') }} s on l.trip_hk = s.trip_hk
)

select
    trip_hk,
    vendor_hk,
    pickup_location_hk,
    dropoff_location_hk,
    cast(date_trunc('day', pickup_datetime) as date) as pickup_date,
    pickup_datetime,
    dropoff_datetime,
    datediff('minute', pickup_datetime, dropoff_datetime) as trip_duration_minutes,
    passenger_cnt,
    trip_distance,
    ratecode_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    case when fare_amount > 0 then round(tip_amount / fare_amount * 100, 2) else null end as tip_pct
from trip_facts
