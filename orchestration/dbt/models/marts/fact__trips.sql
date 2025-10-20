{{ 
    config(
        materialized='incremental'
    )
}}
WITH green_trips AS(
    SELECT 
        *
    FROM {{ ref('int__green_trips') }}
),
yellow_trips AS (
    SELECT 
        *
    FROM {{ ref('int__yellow_trips') }}
),
fhvhv_trips AS (
    SELECT 
        *
    FROM {{ ref('int__fhvhv_trips') }}
),
date_dim AS (
    SELECT * FROM {{ ref('dim__date') }}
),
trips AS 
(
    SELECT * FROM green_trips
    UNION
    SELECT * FROM yellow_trips
    UNION
    SELECT * FROM fhvhv_trips
),
transformed1 AS 
(
    SELECT 
        trips.*,
        date_dim.date_sk AS pickup_date_key
    FROM trips
    JOIN date_dim ON TO_DATE(trips.pickup_datetime) = date_dim.date_day
),
transformed2 AS 
(
    SELECT 
        transformed1.*,
        date_dim.date_sk AS dropoff_date_key
    FROM transformed1
    JOIN date_dim ON TO_DATE(transformed1.dropoff_datetime) = date_dim.date_day
)



SELECT * FROM transformed2
{% if is_incremental() %}
WHERE _etl_loaded_at >= (SELECT MAX(_etl_loaded_at) from {{ this }}) 
{% endif %}