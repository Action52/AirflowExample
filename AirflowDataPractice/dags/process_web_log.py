from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import datetime
import os
import tarfile


def scan_for_log_func():
    # Scan the folder and check if there is any log file
    assert any(_file for _file in os.listdir("/the_logs") if "log.txt" in _file) is True


def extract_data_func():
    with open("/the_logs/log.txt", "r") as logsfile:
        with open("/the_logs/extracted_data.txt", "w") as extractedfile:
            for i, line in enumerate(logsfile):
                try:
                    ip = line.split(" -")[0]
                    extractedfile.write(ip+"\n")
                except Exception as e:
                    continue


def transform_data_func():
    with open("/the_logs/extracted_data.txt") as extractedfile:
        with open("/the_logs/transformed_data.txt", "w") as transformedfile:
            for i, ip in enumerate(extractedfile):
                try:
                    if ip != "198.46.149.143\n":
                        transformedfile.write(ip)
                except Exception as e:
                    continue


def load_data_func():
    name_of_file = "/the_logs/weblog.tar"
    file = tarfile.open(name_of_file, "w")
    file.add("/the_logs/transformed_data.txt")
    file.close()


default_args = {
    'owner': 'Luis Alfredo Leon and Jezuela Gega',
    'depends_on_past': False,
    'start_date': datetime.datetime.now(),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'catchup': False
}

# Create a DAG named process_web_log that runs daily
process_web_log_dag = DAG(
    "process_web_log",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_args
)

task_start_dag = DummyOperator(
    task_id="start_dag",
    dag=process_web_log_dag
)

task_scan_for_log = PythonOperator(
    dag=process_web_log_dag,
    task_id="scan_for_log",
    python_callable=scan_for_log_func
)

task_extract_data = PythonOperator(
    dag=process_web_log_dag,
    task_id="extract_data",
    python_callable=extract_data_func
)

task_transform_data = PythonOperator(
    dag=process_web_log_dag,
    task_id="transform_data",
    python_callable=transform_data_func
)

task_load_data = PythonOperator(
    dag=process_web_log_dag,
    task_id="load_data",
    python_callable=load_data_func
)

task_end_dag = DummyOperator(
    task_id="end_dag",
    dag=process_web_log_dag
)

task_start_dag >> task_scan_for_log >> task_extract_data >> task_transform_data >> task_load_data >> task_end_dag

