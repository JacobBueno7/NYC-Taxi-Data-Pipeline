{{ config(materialized='incremental', unique_key='vendor_hk') }}

with source_data as (
    select
        vendor_hk,
        vendor_id,
        load_date,
        record_source
    from {{ ref('stg_dv_yellow_taxi') }}
    qualify row_number() over (partition by vendor_hk order by load_date) = 1
)

select * from source_data

{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t where t.vendor_hk = source_data.vendor_hk
)
{% endif %}
