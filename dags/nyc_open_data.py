from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta
import airflow
import csv
import requests


default_args = {
    'owner': 'Norton_Li',
    'start_date': datetime.now(),
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    dag_id = 'nyc_open_data',
    default_args=default_args,
    description='NYC public open data',
    )


def get_housing_data():
    """
    NYC public housing location dataset
    """
    nyc_url = 'https://data.cityofnewyork.us/api/views/hg8x-zxpr/rows.csv?accessType=DOWNLOAD'
    response = requests.get(nyc_url)

    with open('nyc_housing_data1.csv', 'wb') as file:
        file.write(response.content)


t1a = PythonOperator(
    task_id = 'get_nyc_housing_data',
    python_callable=get_housing_data,
    provide_context=False,
    dag=dag,
)


def get_shooting_data():
    """
    NYC shoot incident dataset (year of 2019)
    """
    shooting_url = 'https://data.cityofnewyork.us/api/views/5ucz-vwe8/rows.csv?accessType=DOWNLOAD&api_foundry=true'
    response2 = requests.get(shooting_url)

    with open('nyc_shooting_data.csv', 'wb') as file2:
        file2.write(response2.content)


t1b = PythonOperator(
    task_id='get_nyc_shooting_data',
    python_callable=get_shooting_data,
    provide_context=False,
    dag=dag,
)



)