from airflow.sdk import dag, task
import pendulum
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models.param import Param
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.decorators import task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import os
import json
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtDag, DbtTaskGroup
from cosmos.profiles import SnowflakeEncryptedPrivateKeyFilePemProfileMapping
from datetime import datetime
from airflow.hooks.base import BaseHook


DBT_PROJECT_PATH = "/usr/local/airflow/dbt"
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv/bin/dbt"

_project_config = ProjectConfig(
    project_name = "nyc_taxi_analytics",
    dbt_project_path = DBT_PROJECT_PATH,
    env_vars = {"DBT_TARGET": "dev"}
)

conn = BaseHook.get_connection('snowflake_conn')
extra_dict = json.loads(conn.extra) if conn.extra else {}

_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping =SnowflakeEncryptedPrivateKeyFilePemProfileMapping(
        conn_id='snowflake_conn',
        profile_args={
            "account": extra_dict.get("account"),
            "user": conn.login,
            "warehouse": extra_dict.get("warehouse"),
            "database": extra_dict.get("database"),
            "schema": conn.schema,
            "private_key_passphrase": conn.password if conn.password else None,
            "private_key_path": extra_dict.get("private_key_file")
        }
    )
)

_execution_config = ExecutionConfig(
    dbt_executable_path = DBT_EXECUTABLE_PATH
)

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
        },
    template_searchpath = ["/usr/local/aiflow/dags", "/usr/local/airflow/include/sql"]  
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
    
    copy_yellow = SQLExecuteQueryOperator(
        task_id="copy_into_snowflake_yellow",
        conn_id="snowflake_conn",
        sql = "yellow_ingest.sql"
    )
    copy_green = SQLExecuteQueryOperator(
        task_id="copy_into_snowflake_green",
        conn_id="snowflake_conn",
        sql = "green_ingest.sql"
    )
    copy_fhvhv= SQLExecuteQueryOperator(
        task_id="copy_into_snowflake_fhvhv",
        conn_id="snowflake_conn",
        sql = "fhvhv_ingest.sql"
    )
    
    dbt_transformation_test = DbtTaskGroup(
    group_id="dbt_transformation_test",
    project_config=_project_config,
    profile_config=_profile_config,
    execution_config=_execution_config,
    default_args={
        "retries": 2,
    }
    )
    
    end = EmptyOperator(task_id="end")
    
    start >> check_if_yellow >> upload_yellow >> check_if_green >> upload_green >> check_if_fhvhv >> upload_fhvhv >> [copy_yellow, copy_green, copy_fhvhv] >> dbt_transformation_test >> end

    
extract_load()