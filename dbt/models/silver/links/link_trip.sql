{{ config(materialized='incremental', unique_key='trip_link_hk') }}

with source_data as (
    select
        md5(trip_hk || '||' || vendor_hk || '||' || pickup_location_hk || '||' || dropoff_location_hk) as trip_link_hk,
        trip_hk,
        vendor_hk,
        pickup_location_hk,
        dropoff_location_hk,
        load_date,
        record_source
    from {{ ref('stg_dv_yellow_taxi') }}
    qualify row_number() over (partition by trip_hk order by load_date) = 1
)

select * from source_data

{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t where t.trip_link_hk = source_data.trip_link_hk
)
{% endif %}
