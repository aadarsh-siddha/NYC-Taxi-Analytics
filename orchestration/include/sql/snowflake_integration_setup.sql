CREATE STORAGE INTEGRATION IF NOT EXISTS nyc_taxi_s3_bucket
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::042740341370:role/snowflake_role_for_s3'
  STORAGE_ALLOWED_LOCATIONS = ('s3://nyctaxianalytics/')
  STORAGE_AWS_EXTERNAL_ID = '0000'

DESC INTEGRATION nyc_taxi_s3_bucket;

GRANT CREATE STAGE ON SCHEMA nyc_taxi_analytics.bronze TO ROLE ACCOUNTADMIN;

GRANT USAGE ON INTEGRATION nyc_taxi_s3_bucket TO ROLE ACCOUNTADMIN;

USE SCHEMA nyc_taxi_analytics.bronze;

CREATE STAGE my_s3_stage
  STORAGE_INTEGRATION = nyc_taxi_s3_bucket
  URL = 's3://nyctaxianalytics/';