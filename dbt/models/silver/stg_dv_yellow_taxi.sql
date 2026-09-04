with trips as (
    select
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_cnt,
        trip_distance,
        ratecode_id,
        pickup_loc_id,
        dropoff_loc_id,
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
        _loaded_at,
        _record_source
    from {{ ref('bronze_yellow_taxi') }}
    where passenger_cnt > 0
      and trip_distance > 0
      and year(pickup_datetime) >= 2020
),

keyed as (
    select
        *,
        upper(trim(cast(vendor_id as varchar))) || '||' ||
            upper(trim(cast(pickup_datetime as varchar))) || '||' ||
            upper(trim(cast(dropoff_datetime as varchar))) || '||' ||
            upper(trim(cast(pickup_loc_id as varchar))) || '||' ||
            upper(trim(cast(dropoff_loc_id as varchar))) || '||' ||
            upper(trim(cast(trip_distance as varchar))) || '||' ||
            upper(trim(cast(fare_amount as varchar))) as trip_bk
    from trips
)

select
    md5(trip_bk) as trip_hk,
    trip_bk,
    md5(upper(trim(cast(vendor_id as varchar)))) as vendor_hk,
    md5(upper(trim(cast(pickup_loc_id as varchar)))) as pickup_location_hk,
    md5(upper(trim(cast(dropoff_loc_id as varchar)))) as dropoff_location_hk,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_cnt,
    trip_distance,
    ratecode_id,
    pickup_loc_id,
    dropoff_loc_id,
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
    md5(
        coalesce(cast(passenger_cnt as varchar), '') || '||' ||
        coalesce(cast(ratecode_id as varchar), '') || '||' ||
        coalesce(cast(payment_type as varchar), '') || '||' ||
        coalesce(cast(fare_amount as varchar), '') || '||' ||
        coalesce(cast(extra as varchar), '') || '||' ||
        coalesce(cast(mta_tax as varchar), '') || '||' ||
        coalesce(cast(tip_amount as varchar), '') || '||' ||
        coalesce(cast(tolls_amount as varchar), '') || '||' ||
        coalesce(cast(improvement_surcharge as varchar), '') || '||' ||
        coalesce(cast(congestion_surcharge as varchar), '') || '||' ||
        coalesce(cast(airport_fee as varchar), '') || '||' ||
        coalesce(cast(total_amount as varchar), '')
    ) as trip_details_hashdiff,
    _loaded_at as load_date,
    _record_source as record_source
from keyed
