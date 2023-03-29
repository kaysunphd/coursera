from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta


# Default settings applied to all tasks
default_args = {
    "owner": "shadypunk",
    "start_date": days_ago(0),
    "email": ['shadypunkgit@gmail.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# define the DAG
dag = DAG(
    'capstone-ETL',
    default_args=default_args,
    description='capstone ETL',
    schedule_interval=timedelta(days=1),
)

# define the task 'extract data'
extract_data = BashOperator(
    task_id='extract_data',
    bash_command='cut -d" " -f1 /home/project/airflow/dags/capstone/accesslog.txt > /home/project/airflow/dags/capstone/extracted_data.txt',
    dag=dag,
)

# define the task 'transform data'
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='cat /home/project/airflow/dags/capstone/extracted_data.txt | grep "198.46.149.143" > /home/project/airflow/dags/capstone/transform_data.txt',
    dag=dag,
)

# define the task 'load data'
load_data = BashOperator(
    task_id='load_data',
    bash_command='tar -cvf /home/project/airflow/dags/capstone/weblog.tar /home/project/airflow/dags/capstone/transform_data.txt',
    dag=dag,
)

# task pipeline
extract_data >> transform_data >> load_data