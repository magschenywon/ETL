#import the librabries:

from datetime import timedelta 
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago 

#DAG arguments:
default_args = {
    'owner': 'Ramesh Sannareddy',
    'start_date': days_ago(0),
    'email': ['ramesh@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, #try one if it's failed
    'retry_delay': timedelta(minutes=5), #retry after 5 mins
}

#define the DAG
dag = DAG(
    'my-first-dag',
    default_args=default_args,
    description='My first DAG',
    schedule_interval=timedelta(days=1), #dag will run daily
)

#define the tasks

#first task:
extract = BashOperator(
    task_id='extract',
    bash_command='cut -d":" -f1,3,6 /etc/passwd > /home/project/airflow/dags/extracted-data.txt',
    dag=dag,
)

#second task:
transform_and_load = BashOperator(
    task_id='transform',
    bash_command='tr ":" "," < /home/project/airflow/dags/extracted-data.txt > /home/project/airflow/dags/transformed-data.csv',
    dag=dag,
)

# task pipeline
extract >> transform_and_load

# submit the dag :
# cp my_first_dag.py $AIRFLOW_HOME/dags

# list out existing dags:
# airflow dags list

# verify that my-first-dag is part of the output:
# airflow dags list|grep "my-first-dag"

# list out all the tasks in my-first-dag:
# airflow tasks list my-first-dag
