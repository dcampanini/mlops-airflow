from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

#Define default arguments
default_args = {
    "owner": "DiegoCampanini",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": datetime.timedelta(minutes=50),
    "start_date": YESTERDAY,
}

# Instantiate your DAG
tags = ['uai', 'mlops']
dag = DAG(
        dag_id = 'example_dag1', 
        tags = tags,
        default_args=default_args, 
        schedule_interval= '0 8 * * wed'
    )

# Define tasks
def task1():
 print ("Executing Task 1")

def task2():
 print ("Executing Task 2")

task_1 = PythonOperator(
 task_id='task_1',
 python_callable=task1,
 dag=dag,
)
task_2 = PythonOperator(
 task_id='task_2',
 python_callable=task2,
 dag=dag,
)

# Set task dependencies
task_1 >> task_2