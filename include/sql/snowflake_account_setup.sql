CREATE ROLE IF NOT EXISTS airflow_snowflake;

CREATE WAREHOUSE IF NOT EXISTS  TRANSFORMING
WITH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'XS warehouse for transformations';

GRANT OWNERSHIP ON WAREHOUSE TRANSFORMING TO ROLE airflow_snowflake;

USE WAREHOUSE TRANSFORMING;
-- Create database
CREATE DATABASE IF NOT EXISTS nyc_taxi_analytics;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS nyc_taxi_analytics.bronze;
CREATE SCHEMA IF NOT EXISTS nyc_taxi_analytics.silver;
CREATE SCHEMA IF NOT EXISTS nyc_taxi_analytics.gold;

-- Grant database usage to role (corrected syntax)
GRANT USAGE ON DATABASE nyc_taxi_analytics TO ROLE airflow_snowflake;
GRANT CREATE STAGE ON SCHEMA nyc_taxi_analytics.bronze TO ROLE airflow_snowflake;
GRANT USAGE ON INTEGRATION nyc_taxi_s3_bucket TO ROLE airflow_snowflake;
-- Grant schema usage and privileges
GRANT USAGE ON SCHEMA nyc_taxi_analytics.bronze TO ROLE airflow_snowflake;
GRANT USAGE ON SCHEMA nyc_taxi_analytics.silver TO ROLE airflow_snowflake;
GRANT USAGE ON SCHEMA nyc_taxi_analytics.gold TO ROLE airflow_snowflake;

-- Grant privileges for creating and manipulating objects
GRANT CREATE TABLE ON SCHEMA nyc_taxi_analytics.bronze TO ROLE airflow_snowflake;
GRANT CREATE TABLE ON SCHEMA nyc_taxi_analytics.silver TO ROLE airflow_snowflake;
GRANT CREATE TABLE ON SCHEMA nyc_taxi_analytics.gold TO ROLE airflow_snowflake;


GRANT ROLE airflow_snowflake TO USER <your_snowflake_username>;

CREATE STORAGE INTEGRATION IF NOT EXISTS nyc_taxi_s3_bucket
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/YourSnowflakeAccessRole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://nyctaxianalytics/')
  STORAGE_AWS_EXTERNAL_ID = '0000'

USE SCHEMA nyc_taxi_analytics.bronze;

CREATE STAGE my_s3_stage
  STORAGE_INTEGRATION = nyc_taxi_s3_bucket
  URL = 's3://nyctaxianalytics/';


GRANT USAGE ON STAGE nyc_taxi_analytics.bronze.my_s3_stage 
TO ROLE airflow_snowflake;


