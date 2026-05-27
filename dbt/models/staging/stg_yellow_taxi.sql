select 
    VendorID as vendor_id,
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    passenger_count as passenger_cnt,
    trip_distance,
    PULocationID as pickup_loc_id,
    DOLocationID as dropoff_loc_id,
    fare_amount,
    tip_amount,
    total_amount
from {{ source('raw', 'yellow_taxi') }}
where passenger_count > 0 and trip_distance > 0 and year(tpep_pickup_datetime) >= 2020