WITH green_trips AS(
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
        trip_time
    FROM {{ ref('stg__green_trips') }}
),
yellow_trips AS (
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
        trip_time
    FROM {{ ref('stg__yellow_trips') }}
),
fhvhv_trips AS (
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
        trip_time
    FROM {{ ref('stg__fhvhv_trips') }}
),
trips AS 
(
    SELECT * FROM green_trips
    UNION
    SELECT * FROM yellow_trips
    UNION
    SELECT * FROM fhvhv_trips
)

SELECT * FROM trips