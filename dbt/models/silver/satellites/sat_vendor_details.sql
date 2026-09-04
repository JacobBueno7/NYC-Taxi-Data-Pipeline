{{ config(materialized='incremental', unique_key='vendor_hk') }}

with source_data as (
    select
        md5(upper(trim(cast(vendor_id as varchar)))) as vendor_hk,
        vendor_name,
        md5(coalesce(vendor_name, '')) as vendor_details_hashdiff,
        current_timestamp as load_date,
        'seed_vendor_lookup' as record_source
    from {{ ref('vendor_lookup') }}
)

select * from source_data

{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t
    where t.vendor_hk = source_data.vendor_hk
      and t.vendor_details_hashdiff = source_data.vendor_details_hashdiff
)
{% endif %}
