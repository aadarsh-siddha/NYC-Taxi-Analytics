SELECT 
    CASE 
        WHEN hvfhs_license_num = 'HV0002' THEN 'juno'
        WHEN hvfhs_license_num = 'HV0003' THEN 'uber'
        WHEN hvfhs_license_num = 'HV0004' THEN 'via'
        WHEN hvfhs_license_num = 'HV0005' THEN 'lyft'
    END AS taxi_service,
    base_passenger_fare AS base_fare,
    (base_passenger_fare + bcf + sales_tax + congestion_surcharge + airport_fee + tolls + driver_pay + tips)AS total_amount,
    tips AS tip_amount,
    congestion_surcharge,                   
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

                 
    bcf,                   
    sales_tax,             
    airport_fee,            
    tolls,                 
    driver_pay,             


    access_a_ride_flag,     
    shared_request_flag,    
    shared_match_flag,     
    wav_request_flag,       
    wav_match_flag         

FROM {{ source('bronze', 'fhvhv_trips') }}

{% if target.name == 'dev' %}
limit 1000000
{% endif %}