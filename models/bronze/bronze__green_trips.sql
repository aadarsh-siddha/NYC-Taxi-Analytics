SELECT 

    'green_taxi' AS taxi_service,
    fare_amount AS base_fare,
    total_amount,
    tip_amount,
    congestion_surcharge,                   
    pu_location_id,         
    do_location_id,
    lpep_pickup_datetime AS pickup_datetime,       
    lpep_dropoff_datetime AS dropoff_datetime,
    trip_distance,
    DATEDIFF('second', lpep_pickup_datetime, lpep_dropoff_datetime) AS trip_time,

    vendor_id,              
    ratecode_id,            
    passenger_count,        

    payment_type,
    extra,
    mta_tax,
    tolls_amount,
    improvement_surcharge,

    trip_type,
    store_and_fwd_flag

FROM {{ source('bronze', 'green_trips') }}

{% if target.name == 'dev' %}
limit 1000000
{% endif %}