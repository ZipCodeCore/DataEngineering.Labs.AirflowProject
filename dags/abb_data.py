from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import airflow
import csv
import requests


default_args = {
    'owner': 'Norton_Li3',
    'start_date': datetime.now(),
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    dag_id='airbnb_nyc_housing_data',
    default_args=default_args,
    description='Airbnb NYC housing open data of 2019',
    )


def get_abb_data():
    """
    Airbnb dataset of 2019
    """
    nyc_url = 'https://apache-airflow-project.s3.amazonaws.com/AB_NYC_2019.csv'
    response = requests.get(nyc_url)
    with open('abb_nyc_housing_2019_data.csv', 'wb') as file:
        file.write(response.content)


t1 = PythonOperator(
    task_id='get_nyc_housing_data',
    python_callable=get_abb_data,
    provide_context=False,
    dag=dag,
)