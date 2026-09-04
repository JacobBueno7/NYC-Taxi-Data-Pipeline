{{ config(materialized='incremental', unique_key='trip_hk') }}

with source_data as (
    select
        trip_hk,
        trip_bk,
        load_date,
        record_source
    from {{ ref('stg_dv_yellow_taxi') }}
    qualify row_number() over (partition by trip_hk order by load_date) = 1
)

select * from source_data

{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t where t.trip_hk = source_data.trip_hk
)
{% endif %}
