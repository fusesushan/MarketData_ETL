import json 
import csv
import os
from datetime import datetime
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator  
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable


with DAG(
    dag_id = 'market_dag',
    # schedule_interval = '0 * * * *',
    start_date = datetime(2023,10,27),
    catchup = False,
) as dag:


    virtual_env_activate_task = BashOperator(
        task_id='virtual_env_activate',
        bash_command=f"source {Variable.get('base_dir')}mid_term_proj/bin/activate",
    )

    command_exec = f'''
        python3 {Variable.get('base_dir')}data_extraction.py &&
        date "+%s" > {Variable.get('base_dir')}timestamp.txt
    '''

    data_extraction_task = BashOperator(
        task_id = 'data_extraction',
        bash_command =  command_exec,
        # provide_context = True,
    )

    check_file_task = FileSensor(
        task_id = "check_for_file",
        filepath= f"{Variable.get('base_dir')}extracted_data/extracted_data.parquet",
        poke_interval=15, 
        timeout=30,  
        mode='poke' 
    )
 

    File_path = f"{Variable.get('base_dir')}extracted_data/extracted_data.parquet"

    # Function to check if the file content has changed
    def check_file_change(**kwargs):
        # previous_timestamp = kwargs['ti'].xcom_pull(task_ids='data_extraction', key='timestamp')

        current_timestamp = int(os.path.getmtime(File_path))
        print("Current Timestamp: ", str(current_timestamp))
        with open(f"{Variable.get('base_dir')}timestamp.txt", 'r') as file:
            previous_timestamp = int(file.readline())


        print("previous_timestamp: ", str(previous_timestamp))
        if abs(current_timestamp - previous_timestamp) <= 9:
            # File content has changed, do something
            print("File content has changed. Trigger the next task.")
            return 'transform_load_task'
        else:
            # File content has not changed
            print("File content has not changed.")
            return 'skip_transform_load_task'

        # kwargs['ti'].xcom_push(key='timestamp', value=current_timestamp)

    check_file_change_task = BranchPythonOperator(
        task_id='check_file_change',
        provide_context=True,
        python_callable=check_file_change,
        op_args=[],
        )

  
    transform_load_task = BashOperator(
        task_id='transform_load',
        bash_command=f"{Variable.get('spark_submit_path')} --driver-class-path {Variable.get('postgres_jar_file_path')} {Variable.get('base_dir')}transform_and_load_data.py",
    )

    skip_transform_load_task = DummyOperator(
        task_id='skip_transform_load_task',
    )

virtual_env_activate_task >> data_extraction_task >> check_file_task >> check_file_change_task >> [transform_load_task, skip_transform_load_task] 

