{% set start_year = var('start_year', 2024) %}
{% set end_year_exclusive = var('end_year_exclusive', 2026) %}


SELECT 
    {{ dbt_utils.generate_surrogate_key([
      "'green'",                         
      'to_varchar(lpep_pickup_datetime)',
      'to_varchar(lpep_dropoff_datetime)',
      'pu_location_id',
      'do_location_id',
      'trip_distance',
    ]) }} as trip_id,
    'green_taxi' AS taxi_service,
    ABS(fare_amount) AS base_fare,
    ABS(total_amount) AS total_amount,
    ABS(tip_amount) AS tip_amount,
    ABS(congestion_surcharge) AS congestion_surcharge,                   
    pu_location_id,         
    do_location_id,
    lpep_pickup_datetime AS pickup_datetime,       
    lpep_dropoff_datetime AS dropoff_datetime,
    trip_distance,
    DATEDIFF('second', lpep_pickup_datetime, lpep_dropoff_datetime) AS trip_time,

    vendor_id,              
    ratecode_id,            
    passenger_count,        

    ABS(payment_type) AS payment_type,
    ABS(extra) AS extra,
    ABS(mta_tax) AS mta_tax,
    ABS(tolls_amount) AS tolls_amount,
    ABS(improvement_surcharge) AS improvement_surcharge,

    trip_type,
    store_and_fwd_flag

FROM {{ source('bronze', 'green_trips') }}
WHERE
    lpep_pickup_datetime  >= to_timestamp_ntz('{{ start_year }}-01-01')
    and lpep_pickup_datetime  <  to_timestamp_ntz('{{ end_year_exclusive }}-01-01')
    and lpep_dropoff_datetime >= to_timestamp_ntz('{{ start_year }}-01-01')
    and lpep_dropoff_datetime <  to_timestamp_ntz('{{ end_year_exclusive }}-01-01')


{% if target.name == 'dev' %}
limit 1000000
{% endif %}