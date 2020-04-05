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
    dag_id='get_data_url',
    default_args=default_args,
    description='NYC public open data',
    )


def get_housing_data():
    """
    NYC public housing location dataset
    """
    nyc_housing_url = 'https://data.cityofnewyork.us/api/views/hg8x-zxpr/rows.csv?accessType=DOWNLOAD'
    response = requests.get(nyc_housing_url)
    with open('/Users/nli/dev/airflow_home/data/nyc_housing_data.csv', 'wb') as file:
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
    with open('/Users/nli/dev/airflow_home/data/nyc_shooting_data.csv', 'wb') as file2:
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
    with open('/Users/nli/dev/airflow_home/data/nyc_hot_spot.csv', 'wb') as file3:
        file3.write(response3.content)


t3 = PythonOperator(
    task_id='get_nyc_hot_spot',
    python_callable=get_nyc_hot_spot,
    provide_context=False,
    dag=dag,
)


def get_nyc_hotel():
    """
    Free NYC hotels dataset Citywide
    """
    nyc_hotel_url = 'https://data.cityofnewyork.us/api/views/tjus-cn27/rows.csv?accessType=DOWNLOAD&api_foundry=true'
    response4 = requests.get(nyc_hotel_url)
    with open('/Users/nli/dev/airflow_home/data/nyc_hotel_data.csv', 'wb') as file4:
        file4.write(response4.content)


t4 = PythonOperator(
    task_id='get_nyc_hotels',
    python_callable=get_nyc_hotel,
    provide_context=False,
    dag=dag,
)


def get_abb_data():
    """
    Airbnb housing information dataset of 2019
    """
    abb_url = 'https://apache-airflow-project.s3.amazonaws.com/AB_NYC_2019.csv'
    response5 = requests.get(abb_url)
    with open('/Users/nli/dev/airflow_home/data/abb_nyc_housing_2019_data.csv', 'wb') as file5:
        file5.write(response5.content)


t5 = PythonOperator(
    task_id='get_abb_housing_data',
    python_callable=get_abb_data,
    provide_context=False,
    dag=dag,
)


def get_nyc_park_data():
    """
    Airbnb housing information dataset of 2019
    """
    park_url = 'https://data.cityofnewyork.us/api/views/ghu2-eden/rows.csv?accessType=DOWNLOAD'
    response6 = requests.get(park_url)
    with open('/Users/nli/dev/airflow_home/data/nyc_park_data.csv', 'wb') as file6:
        file6.write(response6.content)


t6 = PythonOperator(
    task_id='get_nyc_park',
    python_callable=get_nyc_park_data,
    provide_context=False,
    dag=dag,
)


t1 >> [t2, t3, t4, t5, t6]