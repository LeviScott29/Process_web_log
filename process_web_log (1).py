# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#Define DAG arguments

default_args = {
    'owner': 'levi Scott',
    'start_date': days_ago(0),
    'email': ['somebody@mail.com'],
    'retries' : 1,
    'retry_delay' : timedelta(minutes =5),
}
#define DAG
dag= DAG(
    dag_id= 'process_web_log',
    default_args=default_args,
    description= 'capstone dag',
    schedule_interval=timedelta (days=1),

)

#define extraction step
extract_data= BashOperator(
    task_id= 'extract_data',
    bash_command= 'cut -c 1-12 /home/project/airflow/dags/capstone/accesslog.txt > /home/project/airflow/dags/capstone/extracted_data.txt', 
    dag=dag, 
)

#define transform step
transform_data=BashOperator(
    task_id = 'transform_data',
    bash_command= 'sed "/198\.46\.149\.143/d" /home/project/airflow/dags/capstone/extracted_data.txt > /home/project/airflow/dags/capstone/transformed_data.txt',
    dag=dag, 
)
#define load step
load_data= BashOperator(
    task_id = 'load_data',
    bash_command= 'tar -cvf /home/project/airflow/dags/capstone/weblog.tar /home/project/airflow/dags/capstone/transformed_data.txt',
    dag=dag,
)   

#define pipeline
extract_data >> transform_data >> load_data