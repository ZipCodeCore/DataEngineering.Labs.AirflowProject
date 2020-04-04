from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta
import airflow
import csv
import requests


default_args={
    'owner': 'Norton_Li',
    'start_date': datetime.now(),
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    dag_id='nyc_open_data',
    default_args=default_args,
    description='NYC public open data',
    )


def get_housing_data():
    """
    NYC public housing location dataset
    """
    nyc_url = 'https://data.cityofnewyork.us/api/views/hg8x-zxpr/rows.csv?accessType=DOWNLOAD'
    response = requests.get(nyc_url)
    with open('nyc_housing_data.csv', 'wb') as file:
        file.write(response.content)


t1 = PythonOperator(
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


t2 = PythonOperator(
    task_id='get_nyc_shooting_data',
    python_callable=get_shooting_data,
    provide_context=False,
    dag=dag,
)


def get_nyc_hot_spot():
    """
    Free NYC Public hotspot dataset
    """
    hot_spot_url = 'https://data.cityofnewyork.us/api/views/varh-9tsp/rows.csv?accessType=DOWNLOAD'
    response3 = requests.get(hot_spot_url)
    with open('nyc_hot_spot.csv', 'wb') as file3:
        file3.write(response3.content)


t3 = PythonOperator(
    task_id='get_nyc_hot_spot',
    python_callable=get_nyc_hot_spot,
    provide_context=False,
    dag=dag,
)

t1 >> [t2, t3]