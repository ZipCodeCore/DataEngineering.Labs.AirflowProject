from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from datetime import timedelta
from airflow import DAG
from sqlalchemy import create_engine
import requests
from datetime import datetime

default_args = {
    'owner': 'Norton_Li',
    'start_date': datetime.now(),
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    dag_id='etl_nyc_hotel_data',
    default_args=default_args,
    description='ETL_NYC_hotel_data',
    )


def get_hotel_data():
    """
    NYC hotel information dataset based on 2019
    """
    nyc_hotel_url = 'https://data.cityofnewyork.us/api/views/tjus-cn27/rows.csv?accessType=DOWNLOAD&api_foundry=true'
    response = requests.get(nyc_hotel_url)
    with open('/Users/nli/dev/airflow_home/data/nyc_hotel_data.csv', 'wb') as file:
        file.write(response.content)


t1 = PythonOperator(
    task_id='get_nyc_hotel_data',
    python_callable=get_hotel_data,
    provide_context=False,
    dag=dag,
)


def data_etl():
    """
    Data cleaning with download file and drop some unused columns
    """
    nyc_ht_df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_hotel_data.csv')
    nyc_ht_df = nyc_ht_df.drop(columns=['PARID', 'BOROCODE', 'BLOCK', 'LOT', 'TAXYEAR', 'STREET NUMBER',
                                          'STREET NAME', 'BLDG_CLASS', 'TAXCLASS', 'OWNER_NAME', 'Community Board',
                                          'Council District', 'Census Tract', 'BIN', 'BBL', 'NTA'])
    nyc_ht_df.columns = ['postcode', 'borough', 'latitude', 'longitude']
    nyc_ht_df = nyc_ht_df.set_index('postcode')
    nyc_ht_df.to_csv('/Users/nli/dev/airflow_home/data/nyc_hotel_data2.csv')


t2 = PythonOperator(
    task_id='etl_nyc_hotel',
    python_callable=data_etl,
    provide_context=False,
    dag=dag,
)


def csv_to_mysql():
    conn = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/airflow_project')
    df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_hotel_data2.csv', delimiter=',')
    df.to_sql(name='nyc_hotel', con=conn, schema='airflow_project', if_exists='replace')


t3 = PythonOperator(
        task_id='nyc_hotel_data_to_mysql',
        python_callable=csv_to_mysql,
        dag=dag
)


t1 >> t2 >> t3