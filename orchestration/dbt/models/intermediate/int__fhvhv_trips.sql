{{ 
    config(
        materialized='ephemeral'
    )
}}
SELECT 
    trip_id,
    taxi_service,
    base_fare,
    total_amount,
    tip_amount,
    congestion_surcharge,                   
    pu_location_id,         
    do_location_id,
    pickup_datetime,       
    dropoff_datetime,
    trip_distance,
    trip_time,
    _etl_loaded_at
FROM {{ ref('stg__fhvhv_trips') }}

{% if is_incremental() %}
WHERE _etl_loaded_at >= (SELECT MAX(_etl_loaded_at) from {{ this }}) 
{% endif %}