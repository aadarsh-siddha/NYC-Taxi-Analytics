{% set start_year = var('start_year', 2024) %}
{% set end_year_exclusive = var('end_year_exclusive', 2026) %}

WITH fhvhv_trips AS
(
    SELECT
        {{ dbt_utils.generate_surrogate_key([
        'hvfhs_license_num',                         
        'to_varchar(pickup_datetime)',
        'to_varchar(dropoff_datetime)',
        'pu_location_id',
        'do_location_id',
        'trip_miles',
        ]) }} as trip_id,
        CASE 
            WHEN hvfhs_license_num = 'HV0002' THEN 'juno'
            WHEN hvfhs_license_num = 'HV0003' THEN 'uber'
            WHEN hvfhs_license_num = 'HV0004' THEN 'via'
            WHEN hvfhs_license_num = 'HV0005' THEN 'lyft'
        END AS taxi_service,
        ABS(base_passenger_fare) AS base_fare,
        (ABS(base_passenger_fare) + ABS(bcf) + ABS(sales_tax) + ABS(congestion_surcharge) + ABS(airport_fee) + ABS(tolls) + ABS(driver_pay) + ABS(tips))AS total_amount,
        ABS(tips) AS tip_amount,
        ABS(congestion_surcharge) AS congestion_surcharge,                   
        pu_location_id,         
        do_location_id,
        pickup_datetime,       
        dropoff_datetime,
        trip_miles AS trip_distance,
        trip_time,


        dispatching_base_num,   
        originating_base_num,   
        request_datetime,      
        on_scene_datetime,     

                    
        ABS(bcf) AS bcf,                   
        ABS(sales_tax) AS sales_tax,             
        ABS(airport_fee) AS airport_fee,            
        ABS(tolls) AS tolls,                 
        ABS(driver_pay) AS driver_pay,             


        access_a_ride_flag,     
        shared_request_flag,    
        shared_match_flag,     
        wav_request_flag,       
        wav_match_flag,
        current_timestamp() AS _etl_loaded_at         

    FROM {{ source('bronze', 'fhvhv_trips') }}
    WHERE
        pickup_datetime  >= to_timestamp_ntz('{{ start_year }}-01-01')
        and pickup_datetime  <  to_timestamp_ntz('{{ end_year_exclusive }}-01-01')
        and dropoff_datetime >= to_timestamp_ntz('{{ start_year }}-01-01')
        and dropoff_datetime <  to_timestamp_ntz('{{ end_year_exclusive }}-01-01')
),
deduplicated_fhvhv_trips AS
(
  {{ dbt_utils.deduplicate(
      relation='fhvhv_trips',
      partition_by='trip_id',
      order_by='trip_id ASC',
     )
  }}
)

SELECT * FROM deduplicated_fhvhv_trips