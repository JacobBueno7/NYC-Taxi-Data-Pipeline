{{ config(materialized='incremental', unique_key='trip_hk') }}

with source_data as (
    select
        trip_hk,
        pickup_datetime,
        dropoff_datetime,
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
        trip_details_hashdiff,
        load_date,
        record_source
    from {{ ref('stg_dv_yellow_taxi') }}
    qualify row_number() over (partition by trip_hk order by load_date) = 1
)

select * from source_data

{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t
    where t.trip_hk = source_data.trip_hk
      and t.trip_details_hashdiff = source_data.trip_details_hashdiff
)
{% endif %}
