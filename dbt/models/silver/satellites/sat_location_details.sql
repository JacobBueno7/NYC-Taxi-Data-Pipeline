{{ config(materialized='incremental', unique_key='location_hk') }}

with source_data as (
    select
        md5(upper(trim(cast(location_id as varchar)))) as location_hk,
        borough,
        zone,
        service_zone,
        md5(
            coalesce(borough, '') || '||' || coalesce(zone, '') || '||' || coalesce(service_zone, '')
        ) as location_details_hashdiff,
        _loaded_at as load_date,
        _record_source as record_source
    from {{ ref('bronze_taxi_zone_lookup') }}
)

select * from source_data

{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t
    where t.location_hk = source_data.location_hk
      and t.location_details_hashdiff = source_data.location_details_hashdiff
)
{% endif %}
