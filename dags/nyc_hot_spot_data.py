from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import pandas as pd
from datetime import timedelta
from airflow import DAG
from sqlalchemy import create_engine
import requests
from datetime import datetime
from airflow.operators.papermill_operator import PapermillOperator
import pdfkit as pdf
import papermill as pm

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
    nyc_hs_df.to_csv('/Users/nli/dev/airflow_home/data/nyc_hot_spot_data2.csv')


t2 = PythonOperator(
    task_id='etl_nyc_hot_spot',
    python_callable=data_etl,
    provide_context=False,
    dag=dag,
)


def csv_to_mysql():
    conn = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/airflow_project')
    df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_hot_spot_data2.csv', delimiter=',')
    df.to_sql(name='nyc_hot_spot', con=conn, schema='airflow_project', if_exists='replace')


t3 = PythonOperator(
        task_id='nyc_hot_spot_data_to_mysql',
        python_callable=csv_to_mysql,
        dag=dag
)


t4 = PostgresOperator(
    task_id='create_table_postgres_nyc_hot_spot',
    postgres_conn_id='postgres_nyc_data',
    sql='''CREATE TABLE IF NOT EXISTS nyc_data.nyc_hot_spot(
            type_ varchar(255),
            provider varchar(255),
            latitude float,
            longitude float,
            city varchar(255),
            activated date,
            borough_code integer, 
            borough varchar(255));
            ''',
    dag=dag,


)

path = '/Users/nli/dev/airflow_home/data/nyc_hot_spot_data2.csv'
t5 = PostgresOperator(
    task_id='import_to_postgres',
    postgres_conn_id='postgres_nyc_data',
    sql=f"DELETE FROM nyc_data.nyc_hot_spot; COPY nyc_data.nyc_hot_spot FROM '{path}' DELIMITER ',' CSV HEADER;",
    dag=dag,
)


def get_jupyter():
    pm.execute_notebook('/Users/nli/dev/airflow_home/nyc_hot_spot.ipynb',
                        '/Users/nli/dev/airflow_home/nyc_hot_spot_output.ipynb',
                        parameters={'file_name': '/Users/nli/dev/airflow_home/data/nyc_hot_spot2.csv'}
                        )


t6 = PythonOperator(
    task_id='call_jupyter_report',
    provide_context=False,
    python_callable=get_jupyter,
    dag=dag,
)

t1 >> t2 >> t3,
t1 >> t2 >> t4 >> t5
t1 >> t2 >> t6