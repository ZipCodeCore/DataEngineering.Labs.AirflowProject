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
    dag_id='etl_nyc_park_data',
    default_args=default_args,
    description='ETL_NYC_park_data',
    )


def get_park_data():
    """
    NYC park information dataset based on 2019
    """
    nyc_park_url = 'https://data.cityofnewyork.us/api/views/ghu2-eden/rows.csv?accessType=DOWNLOAD'
    response = requests.get(nyc_park_url)
    with open('/Users/nli/dev/airflow_home/data/nyc_park_data.csv', 'wb') as file:
        file.write(response.content)


t1 = PythonOperator(
    task_id='get_nyc_park_data',
    python_callable=get_park_data,
    provide_context=False,
    dag=dag,
)


def data_etl():
    """
    Data cleaning with download file and drop some unused columns
    """
    nyc_ht_df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_park_data.csv')
    nyc_ht_df = nyc_ht_df.drop(columns=['the_geom', 'GISPROPNUM', 'OBJECTID', 'OMPPROPID', 'DEPARTMENT',
       'PERMITDIST', 'PERMITPARE', 'PARENTID', 'LOCATION','COMMUNITYB','COUNCILDIS', 'PRECINCT', 'ZIPCODE','RETIRED',
       'EAPPLY', 'PIP_RATABL', 'GISOBJID', 'CLASS', 'COMMISSION', 'ACQUISITIO', 'ADDRESS', 'JURISDICTI', 'MAPPED',
        'NAME311', 'PERMIT', 'SIGNNAME','SUBCATEGOR', 'URL','NYS_ASSEMB','NYS_SENATE', 'US_CONGRES', 'GlobalID'])
    nyc_ht_df.columns = ['borough', 'acres', 'typecatego', 'waterfront']
    nyc_ht_df = nyc_ht_df.set_index('borough')
    nyc_ht_df.to_csv('/Users/nli/dev/airflow_home/data/nyc_park_data2.csv')


t2 = PythonOperator(
    task_id='etl_nyc_park',
    python_callable=data_etl,
    provide_context=False,
    dag=dag,
)


def csv_to_mysql():
    conn = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/airflow_project')
    df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_park_data2.csv', delimiter=',')
    df.to_sql(name='nyc_park', con=conn, schema='airflow_project', if_exists='replace')


t3 = PythonOperator(
        task_id='nyc_park_data_to_mysql',
        python_callable=csv_to_mysql,
        dag=dag
)


t1 >> t2 >> t3