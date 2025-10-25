from airflow.sdk import dag, task
import pendulum
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.models.taskinstance import TaskInstance
from airflow.models.dagrun import DagRun
from airflow.operators.python import ShortCircuitOperator
from airflow.decorators import task_group

def check_requested_type(available_type: str, requested_type: str) -> bool:
    if requested_type == available_type:
        return True
    elif requested_type == "all":
        return True
    else:
        return False

@task_group
def data_upload_process(taxi_type: str):
    
    check_endpoint = HttpSensor(
        task_id="check_taxi_data_availability",
        http_conn_id="taxi_data_api",  
        endpoint=f"/trip-data/{taxi_type}_tripdata_{{{{ macros.ds_format(macros.ds_add(ds, -60), '%Y-%m-%d', '%Y-%m') }}}}.parquet",
        method="GET",
        response_check=lambda response: response.status_code == 200,
        poke_interval=30,
        timeout=600,
    )
        
    uploadtoS3direct = HttpToS3Operator(
        task_id="nyc_taxi_http_to_s3",
        http_conn_id="taxi_data_api",                 
        endpoint=f"/trip-data/{taxi_type}_tripdata_{{{{ macros.ds_format(macros.ds_add(ds, -60), '%Y-%m-%d', '%Y-%m') }}}}.parquet",
        s3_bucket="nyctaxianalytics",
        s3_key=f"{taxi_type}/{{{{ macros.ds_format(macros.ds_add(ds, -60), '%Y-%m-%d', '%Y') }}}}/{{{{ macros.ds_format(macros.ds_add(ds, -60), '%Y-%m-%d', '%m') }}}}/{taxi_type}_tripdata_{{{{ macros.ds_format(macros.ds_add(ds, -60), '%Y-%m-%d', '%Y-%m') }}}}.parquet",
        replace=True,
        aws_conn_id="aws_default"
    )
    check_endpoint >> uploadtoS3direct

@dag(
    schedule="0 0 15 * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="UTC"),
    catchup=False,
    description="DAG to extract NYC taxi data from HTTP API and load into S3, COPY INTO Snowflake later. Also, triggers dbt transformations after loading.",
    tags=["extract", "load", "nyc_taxi_analytics"],
    max_active_runs=1,
    params = {
        "taxi": Param("all", type="string", enum=["yellow", "green", "fhvhv", "all"])
        # "year": Param(2024, type="integer", minimum=2024, maximum=2025),
        # "month": Param(1, type="integer", minimum=1, maximum=12)
        }  
)
def extract_load():   
    
    start = EmptyOperator(task_id="start")
    
    check_if_yellow = ShortCircuitOperator(
        task_id="check_if_yellow",
        python_callable=check_requested_type,
        op_args=["yellow", "{{ params.taxi }}"]
    )
    
    check_if_green = ShortCircuitOperator(
        task_id="check_if_green",
        python_callable=check_requested_type,
        op_args=["green", "{{ params.taxi }}"]
    )
    
    check_if_fhvhv = ShortCircuitOperator(
        task_id="check_if_fhvhv",
        python_callable=check_requested_type,
        op_args=["fhvhv", "{{ params.taxi }}"]
    )
    
    upload_yellow = data_upload_process("yellow")
    upload_green = data_upload_process("green")
    upload_fhvhv = data_upload_process("fhvhv")
    
    end = EmptyOperator(task_id="end")
    
    start >> check_if_yellow >> upload_yellow >> check_if_green >> upload_green >> check_if_fhvhv >> upload_fhvhv >> end

    
extract_load()
