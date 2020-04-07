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
    dag_id='etl_nyc_hot_spot_data',
    default_args=default_args,
    description='ETL_NYC_hot_spot_data',
    )


def get_hot_spot_data():
    """
    NYC free hot spot information dataset based on 2019
    """
    nyc_hot_spot_url = 'https://data.cityofnewyork.us/api/views/varh-9tsp/rows.csv?accessType=DOWNLOAD'
    response = requests.get(nyc_hot_spot_url)
    with open('/Users/nli/dev/airflow_home/data/nyc_hot_spot_data.csv', 'wb') as file:
        file.write(response.content)


t1 = PythonOperator(
    task_id='get_nyc_hot_spot_data',
    python_callable=get_hot_spot_data,
    provide_context=False,
    dag=dag,
)


def data_etl():
    """
    Data cleaning with download file and drop some unused columns
    """
    nyc_hs_df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_hot_spot_data.csv')
    nyc_hs_df = nyc_hs_df.drop(columns=['BORO', 'the_geom', 'OBJECTID', 'NAME', 'LOCATION',
        'X', 'Y', 'LOCATION_T', 'REMARKS', 'SSID','SOURCEID', 'NTACODE', 'NTANAME', 'COUNDIST',
        'POSTCODE', 'BOROCD', 'CT2010', 'BOROCT2010', 'BIN', 'BBL', 'DOITT_ID'])
    nyc_hs_df.columns = ['type', 'provider','latitude', 'longitude', 'city', 'activated', 'borough_code','borough' ]
    nyc_hs_df = nyc_hs_df.set_index('type')
    nyc_hs_df.to_csv('/Users/nli/dev/airflow_home/data/nyc_hot_spot_data_2.csv')


t2 = PythonOperator(
    task_id='etl_nyc_hot_spot',
    python_callable=data_etl,
    provide_context=False,
    dag=dag,
)


def csv_to_mysql():
    conn = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/airflow_project')
    df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_hot_spot_data_2.csv', delimiter=',')
    df.to_sql(name='nyc_hot_spot', con=conn, schema='airflow_project', if_exists='replace')


t3 = PythonOperator(
        task_id='nyc_hot_spot_data_to_mysql',
        python_callable=csv_to_mysql,
        dag=dag
)


t1 >> t2 >> t3