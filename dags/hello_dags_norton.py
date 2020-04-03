from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import airflow

def print_hello():
    return 'Hello airflow world'

default_args = {
    'owner': 'Norton_L',
    'start_date': airflow.utils.dates.days_ago(1),
    'depend_on_past': True,
    #with this set to True, the pipline won't run if the previous data failed
    'email': ['info@air.com'],
    'email_on_failure': True,
    #upon failure this pipeline will send an email to your email set above
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

dag=DAG(
'first_dag',
default_args=default_args,
schedule_interval = timedelta(days=1),
)

dummy_operator = DummyOperator(task_id='dummy_task', retries=2, dag=dag)

hello_operator = PythonOperator(task_id='first_id', python_callable=print_hello, dag=dag)

dummy_operator >> hello_operator
