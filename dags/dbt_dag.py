import os
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtDag, DbtTaskGroup
from cosmos.profiles import SnowflakeEncryptedPrivateKeyFilePemProfileMapping
from datetime import datetime


DBT_PROJECT_PATH = "/usr/local/airflow/dbt"
DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv/bin/dbt"

_project_config = ProjectConfig(
    project_name = "nyc_taxi_analytics",
    dbt_project_path = DBT_PROJECT_PATH,
    env_vars = {"DBT_TARGET": "dev"}
)


# profile = SnowflakeEncryptedPrivateKeyFilePemProfileMapping(
#     conn_id = 'my_snowflake_connection',
#     profile_args = {
#         "account": extra.account,
#         "user": "<your_user>",
#         "warehouse": "<your_warehouse>",
#         "database": "<your_database>",
#         "schema": "<your_schema>",
#         "authenticator": "snowflake",
#         "private_key_passphrase": "<your_private_key_passphrase>",
#         "private_key_path": "<path_to_your_private_key_file>"
#     },
# )

_profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
)

_execution_config = ExecutionConfig(
    dbt_executable_path = DBT_EXECUTABLE_PATH
)

my_dag = DbtDag(
    dag_id = "dbt_dag",
    project_config = _project_config,
    profile_config = _profile_config,
    execution_config = _execution_config,
    schedule = "@daily",
    start_date = datetime(2025, 1, 1),
    catchup = False,
    tags = ["dbt"]
)