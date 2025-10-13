{% set start_year = var('start_year', 2024) %}
{% set end_year_exclusive = var('end_year_exclusive', 2026) %}

WITH yellow_trips AS 
(
    SELECT 
        {{ dbt_utils.generate_surrogate_key([
        "'yellow'",                         
        'to_varchar(tpep_pickup_datetime)',
        'to_varchar(tpep_dropoff_datetime)',
        'pu_location_id',
        'do_location_id',
        'trip_distance',
        'tip_amount',
        'passenger_count'
        ]) }} as trip_id,
        'yellow_taxi' AS taxi_service,
        ABS(fare_amount) AS base_fare,
        ABS(total_amount) AS total_amount,
        COALESCE(ABS(tip_amount),0) AS tip_amount,
        ABS(congestion_surcharge) AS congestion_surcharge,                   
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
        ABS(extra) AS extra,
        ABS(mta_tax) AS mta_tax,
        ABS(tolls_amount) AS tolls_amount,
        ABS(improvement_surcharge) AS improvement_surcharge,
        ABS(airport_fee) AS airport_fee, 

        store_and_fwd_flag,    


    FROM {{ source('bronze', 'yellow_trips') }}

    WHERE
        tpep_pickup_datetime  >= to_timestamp_ntz('{{ start_year }}-01-01')
        and tpep_pickup_datetime  <  to_timestamp_ntz('{{ end_year_exclusive }}-01-01')
        and tpep_dropoff_datetime >= to_timestamp_ntz('{{ start_year }}-01-01')
        and tpep_dropoff_datetime <  to_timestamp_ntz('{{ end_year_exclusive }}-01-01')

    {% if target.name == 'dev' %}
    limit 1000000
    {% endif %}
),
deduplicated_yellow_trips AS (
  {{ dbt_utils.deduplicate(
      relation='yellow_trips',
      partition_by='trip_id',
      order_by='trip_id ASC',
     )
  }}
)

SELECT * FROM deduplicated_yellow_trips