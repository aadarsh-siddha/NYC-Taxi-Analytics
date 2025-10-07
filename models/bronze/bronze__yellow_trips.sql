SELECT 
    'yellow_taxi' AS taxi_service,
    fare_amount AS base_fare,
    total_amount,
    tip_amount,
    congestion_surcharge,                   
    pu_location_id,         
    do_location_id,
    tpep_pickup_datetime AS pickup_datetime,       
    tpep_dropoff_datetime AS dropoff_datetime,
    trip_distance,
    DATEDIFF('second', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_time,

    vendor_id,              
    ratecode_id,            
    passenger_count,        

    payment_type,
    extra,
    mta_tax,
    tolls_amount,
    improvement_surcharge,
    airport_fee, 

    store_and_fwd_flag,    


FROM {{ source('bronze', 'yellow_trips') }}

{% if target.name == 'dev' %}
limit 1000000
{% endif %}