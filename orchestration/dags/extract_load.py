from airflow.sdk import dag, task
import pendulum
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator


@dag(
    schedule="0 0 1 * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["extract", "load"],
    params = {
        "taxi": Param("green", type="string", enum=["yellow", "green", "fhvhv"]),
        "year": Param(2025, type="integer", minimum=2024, maximum=2025),
        "month": Param(1, type="integer", minimum=1, maximum=12),
        "download_dir": Param("/tmp/taxi_data")
    }  
)
def extract_load():
    taxi = "{{ params.taxi }}"
    year = "{{ params.year }}"
    month = "{{ '{:02d}'.format(params.month | int) }}"
    endpoint = f"/trip-data/{taxi}_tripdata_{year}-{month}.parquet"
    download_dir = "{{ params.download_dir }}"
    
    check_endpoint = HttpSensor(
        task_id="check_taxi_data_availability",
        http_conn_id="taxi_data_api",  
        endpoint=endpoint,
        method="GET",
        response_check=lambda response: response.status_code == 200,
        poke_interval=30,
        timeout=600,
    )
        
    uploadtoS3direct = HttpToS3Operator(
        task_id="nyc_taxi_http_to_s3",
        http_conn_id="taxi_data_api",                 
        endpoint=f"/trip-data/{ taxi }_tripdata_{ year }-{ month }.parquet",
        s3_bucket="nyctaxianalytics",
        s3_key=f"{ taxi }/{ year }/{ month }/{ taxi }_tripdata_{ year }-{ month }.parquet",
        replace=True,
        aws_conn_id= "aws_default"
    )
    
    @task
    def check_schema_for_upload():
        s3_bucket="nyctaxianalytics",
        s3_key=f"{ taxi }/{ year }/{ month }/{ taxi }_tripdata_{ year }-{ month }.parquet",
        
    check_endpoint >> uploadtoS3direct

extract_load()