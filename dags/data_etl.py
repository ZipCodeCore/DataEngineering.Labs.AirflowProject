import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime
from datetime import timedelta
import requests
import pymysql
import sys


default_args = {
    'owner': 'Norton_Li5',
    'start_date': datetime.now(),
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    dag_id='etl_nyc_housing_data',
    default_args=default_args,
    description='ETL_NYC_housing_data',
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


def data_etl():
    """
    Data cleaning with download file and drop some unused columns
    """
    nyc_h_df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_housing_data.csv')
    nyc_h_df = nyc_h_df.drop(columns=['Program Group', 'Project Name', 'Project Start Date', 'Project Completion Date',
                           'Building ID','Number', 'Street','Postcode','BBL', 'BIN', 'Community Board', 'Council District',
                           'Census Tract', 'NTA - Neighborhood Tabulation Area','Latitude', 'Longitude',
                           'Latitude (Internal)', 'Longitude (Internal)', 'Building Completion Date',
                           'Reporting Construction Type', 'Extended Affordability Only', 'Prevailing Wage Status',
                           'Studio Units','1-BR Units', '2-BR Units', '3-BR Units', '4-BR Units', '5-BR Units',
                           '6-BR+ Units','Unknown-BR Units','Counted Rental Units', 'Counted Homeownership Units',
                           'All Counted Units'])
    nyc_h_df.columns = ['project_id', 'borough', 'extremely_low_income_units', 'very_low_income_units',
                        'low_income_units', 'moderate_income_units', 'middle_income_units',
                        'other_income_units', 'total_units']
    nyc_h_df.to_csv('/Users/nli/dev/airflow_home/data/nyc_housing_data_2.csv')


t2 = PythonOperator(
    task_id='etl_nyc_housing',
    python_callable=data_etl,
    provide_context=False,
    dag=dag,
)


def csv_to_mysql(load_sql, host, user, password):
    """
    This function load a csv file to MySQL table according to
    the load_sql statement.
    """
    try:
        con = pymysql.connect(host=host,
                              user=user,
                              password=password,
                              autocommit=True,
                              local_infile=1)
        print('Connected to DB: {}'.format(host))
        # Create cursor and execute Load SQL
        cursor = con.cursor()
        cursor.execute(load_sql)
        print('Succuessfully loaded the table from csv.')
        con.close()

    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1)


load_sql = "LOAD DATA LOCAL INFILE '/Users/nli/dev/airflow_home/data/nyc_housing_data_2.csv' INTO TABLE airflow_project.nyc_housing FIELDS TERMINATED BY ',' ENCLOSED BY '' IGNORE 1 LINES;"
host = '127.0.0.1'
user = 'root'
password = 'password'
csv_to_mysql(load_sql, host, user, password)

t3 = PythonOperator(
    task_id='csv_to_mysql',
    python_callable=csv_to_mysql,
    provide_context=False,
    dag=dag,
)

t1 >> t2 >> t3
