CREATE TABLE IF NOT EXISTS nyc_taxi_analytics.bronze.green_trips (
  vendor_id              NUMBER(38,0),
  ratecode_id            NUMBER(38,0),
  pu_location_id         NUMBER(38,0),
  do_location_id         NUMBER(38,0),
  passenger_count        NUMBER(38,0),

  trip_distance          NUMBER(10,2),
  lpep_pickup_datetime   TIMESTAMP_NTZ,
  lpep_dropoff_datetime  TIMESTAMP_NTZ,

  payment_type           NUMBER(38,0),
  fare_amount            NUMBER(10,2),
  extra                  NUMBER(10,2),
  mta_tax                NUMBER(10,2),
  tip_amount             NUMBER(10,2),
  tolls_amount           NUMBER(10,2),
  improvement_surcharge  NUMBER(10,2),
  congestion_surcharge   NUMBER(10,2),
  total_amount           NUMBER(10,2),

  trip_type              NUMBER(38,0),
  store_and_fwd_flag     STRING,
  _etl_loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO nyc_taxi_analytics.bronze.green_trips
FROM (
  SELECT
    $1:VendorID::NUMBER,
    $1:RatecodeID::NUMBER,
    $1:PULocationID::NUMBER,
    $1:DOLocationID::NUMBER,
    $1:passenger_count::NUMBER,

    ROUND($1:trip_distance::FLOAT, 2)::NUMBER(10,2),
    TO_TIMESTAMP_NTZ($1:lpep_pickup_datetime::NUMBER / 1000000),
    TO_TIMESTAMP_NTZ($1:lpep_dropoff_datetime::NUMBER / 1000000),

    $1:payment_type::NUMBER,
    ROUND($1:fare_amount::FLOAT, 2)::NUMBER(10,2),
    ROUND($1:extra::FLOAT, 2)::NUMBER(10,2),
    ROUND($1:mta_tax::FLOAT, 2)::NUMBER(10,2),
    ROUND($1:tip_amount::FLOAT, 2)::NUMBER(10,2),
    ROUND($1:tolls_amount::FLOAT, 2)::NUMBER(10,2),
    ROUND($1:improvement_surcharge::FLOAT, 2)::NUMBER(10,2),
    ROUND($1:congestion_surcharge::FLOAT, 2)::NUMBER(10,2),
    ROUND($1:total_amount::FLOAT, 2)::NUMBER(10,2),

    $1:trip_type::NUMBER,
    $1:store_and_fwd_flag::STRING,
    CURRENT_TIMESTAMP()
  FROM @nyc_taxi_analytics.bronze.my_s3_stage/green/2024/01/
)
FILE_FORMAT = (TYPE = PARQUET);
