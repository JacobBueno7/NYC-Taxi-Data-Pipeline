{{ config(materialized='incremental', unique_key='location_hk') }}

with from_zone_lookup as (
    select
        md5(upper(trim(cast(location_id as varchar)))) as location_hk,
        location_id,
        _loaded_at as load_date,
        _record_source as record_source
    from {{ ref('bronze_taxi_zone_lookup') }}
),

from_trips as (
    select pickup_location_hk as location_hk, pickup_loc_id as location_id, load_date, record_source
    from {{ ref('stg_dv_yellow_taxi') }}
    union all
    select dropoff_location_hk as location_hk, dropoff_loc_id as location_id, load_date, record_source
    from {{ ref('stg_dv_yellow_taxi') }}
),

unioned as (
    select * from from_zone_lookup
    union all
    select * from from_trips
),

deduped as (
    select location_hk, location_id, load_date, record_source
    from unioned
    qualify row_number() over (partition by location_hk order by load_date) = 1
)

select * from deduped

{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t where t.location_hk = deduped.location_hk
)
{% endif %}
