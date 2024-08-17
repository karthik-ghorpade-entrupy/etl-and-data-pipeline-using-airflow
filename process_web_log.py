# importing the libraries 

from datetime import timedelta
# DAG object; this is needed to instantiate a DAG
from airflow import DAG
#Operator
from airflow.operators.bash_operator import BashOperator
# making scheduling easy
from airflow.utils.dates import days_ago

# Defining DAG arguments

default_args = {
    'owner': 'kar',
    'start_date': days_ago(0),
    'email': ['karthik@xyz.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delta': timedelta(minutes=5)
}

# Defining the DAG

dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='Dag to process web logs!',
    schedule_interval=timedelta(days=1)
)

# Defining the tasks
#First Task
extract_data = BashOperator(
    task_id='extract_data',
    bash_command='cut -d" " -f1 /home/project/airflow/dags/capstone/accesslog.txt \
    > /home/project/airflow/dags/extracted_data.txt',
    dag=dag
)

#Second Task
transformed_data = BashOperator(
    task_id='transform_data',
    bash_command='sort /home/project/airflow/dags/extracted_data.txt | uniq \
    > /home/project/airflow/dags/transformed_data1.txt && \
    grep -v "198.46.149.143" /home/project/airflow/dags/transformed_data1.txt \
    > /home/project/airflow/dags/transformed_data.txt',
    dag=dag
)

#Third Task
load_data = BashOperator(
    task_id='load_data',
    bash_command=
    'tar -cvf /home/project/airflow/dags/weblog.tar /home/project/airflow/dags/transformed_data.txt',
    dag=dag
)

#Task Pipeline
extract_data >> transformed_data >> load_data