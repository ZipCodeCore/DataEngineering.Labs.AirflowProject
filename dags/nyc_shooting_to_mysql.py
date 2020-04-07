from airflow.operators.python_operator import PythonOperator
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
    dag_id='etl_nyc_shooting_data',
    default_args=default_args,
    description='ETL_NYC_shooting_data',
    )


def get_shooting_data():
    """
    NYC shooting cases dataset 2019
    """
    nyc_shooting_url = 'https://data.cityofnewyork.us/api/views/5ucz-vwe8/rows.csv?accessType=DOWNLOAD&api_foundry=true'
    response = requests.get(nyc_shooting_url)
    with open('/Users/nli/dev/airflow_home/data/nyc_shooting_data.csv', 'wb') as file:
        file.write(response.content)


t1 = PythonOperator(
    task_id = 'get_nyc_shooting_data',
    python_callable=get_shooting_data,
    provide_context=False,
    dag=dag,
)


def data_etl():
    """
    Data cleaning with download file and drop some unused columns
    """
    nyc_s_df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_shooting_data.csv')
    nyc_s_df = nyc_s_df.drop(columns=['INCIDENT_KEY', 'JURISDICTION_CODE', 'LOCATION_DESC',
                                                'STATISTICAL_MURDER_FLAG', 'PERP_AGE_GROUP', 'PERP_SEX', 'PERP_RACE',
                                                'VIC_AGE_GROUP', 'VIC_AGE_GROUP', 'VIC_RACE',
                                                 'X_COORD_CD', 'Y_COORD_CD'])
    nyc_s_df.columns = ['occur_date', 'occur_time', 'borough', 'precinct', 'vic_sex','latitude', 'longitude']
    nyc_s_df = nyc_s_df.set_index('occur_date')
    nyc_s_df.to_csv('/Users/nli/dev/airflow_home/data/nyc_shooting_data2.csv')


t2 = PythonOperator(
    task_id='etl_nyc_shooting',
    python_callable=data_etl,
    provide_context=False,
    dag=dag,
)


def csv_to_mysql():
    conn = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/airflow_project')
    df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_shooting_data2.csv', delimiter=',')
    df.to_sql(name='nyc_shooting', con=conn, schema='airflow_project', if_exists='replace')


t3 = PythonOperator(
        task_id='nyc_shooting_data_to_mysql',
        python_callable=csv_to_mysql,
        dag=dag
)


t1 >> t2 >> t3