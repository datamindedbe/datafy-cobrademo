from airflow import DAG
from datafy_airflow_plugins.datafy_container_plugin.datafy_container_operator import (
    DatafyContainerOperator,
)
from datetime import datetime, timedelta


default_args = {
    "owner": "Datafy",
    "depends_on_past": False,
    "start_date": datetime(year=2021, month=2, day=11),
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


image = "{{ macros.image('cobrademo') }}"

dag = DAG(
    "cobrademo",
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
)

preprocessing = DatafyContainerOperator(
    dag=dag,
    task_id="preprocessing",
    name="preprocessing",
    image=image,
    arguments=["--date", "{{ ds }}", "--jobs", "preprocessing", "--env", "{{ macros.env() }}"],
    service_account_name="cobrademo",
)

pig_tables = DatafyContainerOperator(
    dag=dag,
    task_id="pig_tables",
    name="pig_tables",
    image=image,
    arguments=["--date", "{{ ds }}", "--jobs", "pig_tables", "--env", "{{ macros.env() }}"],
    service_account_name="cobrademo",
)

model_train = DatafyContainerOperator(
    dag=dag,
    task_id="model_train",
    name="model_train",
    image=image,
    arguments=["--date", "{{ ds }}", "--jobs", "model_training", "--env", "{{ macros.env() }}"],
    service_account_name="cobrademo",
)

model_run = DatafyContainerOperator(
    dag=dag,
    task_id="model_run",
    name="model_run",
    image=image,
    arguments=["--date", "{{ ds }}", "--jobs", "model_run", "--env", "{{ macros.env() }}"],
    service_account_name="cobrademo",
)

preprocessing >> pig_tables
preprocessing >> model_train >> model_run