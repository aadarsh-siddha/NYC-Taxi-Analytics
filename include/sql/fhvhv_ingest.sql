CREATE TABLE IF NOT EXISTS nyc_taxi_analytics.bronze.fhvhv_trips (
  hvfhs_license_num      STRING,
  dispatching_base_num   STRING,
  originating_base_num   STRING,

  pu_location_id         NUMBER(38,0),
  do_location_id         NUMBER(38,0),

  request_datetime       TIMESTAMP_NTZ,
  on_scene_datetime      TIMESTAMP_NTZ,
  pickup_datetime        TIMESTAMP_NTZ,
  dropoff_datetime       TIMESTAMP_NTZ,

  trip_miles             NUMBER(10,2),
  trip_time              NUMBER(38,0),    -- seconds
  base_passenger_fare    NUMBER(10,2),
  bcf                    NUMBER(10,2),    -- Black Car Fund fee
  sales_tax              NUMBER(10,2),
  congestion_surcharge   NUMBER(10,2),
  airport_fee            NUMBER(10,2),
  tips                   NUMBER(10,2),
  tolls                  NUMBER(10,2),
  driver_pay             NUMBER(10,2),

  access_a_ride_flag     STRING,
  shared_request_flag    STRING,
  shared_match_flag      STRING,
  wav_request_flag       STRING,
  wav_match_flag         STRING,

  _etl_loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO nyc_taxi_analytics.bronze.fhvhv_trips
FROM (
  SELECT
    $1:hvfhs_license_num::STRING,
    $1:dispatching_base_num::STRING,
    $1:originating_base_num::STRING,

    $1:PULocationID::NUMBER,
    $1:DOLocationID::NUMBER,

    TO_TIMESTAMP_NTZ($1:request_datetime::NUMBER / 1000000),
    TO_TIMESTAMP_NTZ($1:on_scene_datetime::NUMBER / 1000000),
    TO_TIMESTAMP_NTZ($1:pickup_datetime::NUMBER / 1000000),
    TO_TIMESTAMP_NTZ($1:dropoff_datetime::NUMBER / 1000000),

    ROUND($1:trip_miles::FLOAT, 2)::NUMBER(10,2),
    $1:trip_time::NUMBER,
    ROUND($1:base_passenger_fare::FLOAT, 2)::NUMBER(10,2),
    ROUND($1:bcf::FLOAT, 2)::NUMBER(10,2),
    ROUND($1:sales_tax::FLOAT, 2)::NUMBER(10,2),
    ROUND($1:congestion_surcharge::FLOAT, 2)::NUMBER(10,2),
    ROUND($1:airport_fee::FLOAT, 2)::NUMBER(10,2),
    ROUND($1:tips::FLOAT, 2)::NUMBER(10,2),
    ROUND($1:tolls::FLOAT, 2)::NUMBER(10,2),
    ROUND($1:driver_pay::FLOAT, 2)::NUMBER(10,2),

    $1:access_a_ride_flag::STRING,
    $1:shared_request_flag::STRING,
    $1:shared_match_flag::STRING,
    $1:wav_request_flag::STRING,
    $1:wav_match_flag::STRING,
    CURRENT_TIMESTAMP()
  FROM @nyc_taxi_analytics.bronze.my_s3_stage/fhvhv/{{ macros.ds_format(macros.ds_add(ds, -60), '%Y-%m-%d', '%Y') }}/{{ macros.ds_format(macros.ds_add(ds, -60), '%Y-%m-%d', '%m') }}/
)
FILE_FORMAT = (TYPE = PARQUET);
