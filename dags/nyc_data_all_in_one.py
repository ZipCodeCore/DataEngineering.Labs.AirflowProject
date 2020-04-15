from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import pandas as pd
from datetime import timedelta
from airflow import DAG
from sqlalchemy import create_engine
import requests
from datetime import datetime
import papermill as pm
import zipfile
import kaggle


default_args = {
    'owner': 'Norton_Li',
    'start_date': datetime.now(),
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    dag_id='airbnb_nyc_housing_correlation_study',
    default_args=default_args,
    description='Airbnb NYC housing open data and NYC open data of 2019 study',
    )


t0 = DummyOperator(
    task_id='nyc_data_etl_start_here',
    retries=1,
    dag=dag
)

t1a = BashOperator(
    task_id='run_abb_kaggle_api',
    bash_command='kaggle datasets download -d dgomonov/new-york-city-airbnb-open-data'
                 ' -p /Users/nli/dev/airflow_home/data/',
    dag=dag,
)


def unzip():
    zf = zipfile.ZipFile('/Users/nli/Downloads/new-york-city-airbnb-open-data.zip')
    df = pd.read_csv(zf.open('AB_NYC_2019.csv'), encoding='ISO-8859-1')
    df.to_csv('/Users/nli/dev/airflow_home/data/abb_nyc_housing_2019_data.csv')


t1b = PythonOperator(
    task_id='unzip_api_abb',
    python_callable=unzip,
    provide_context=False,
    dag=dag,
)


def abb_data_etl():
    """
    Data cleaning with download file and drop some unused columns
    """
    abb_df = pd.read_csv('/Users/nli/dev/airflow_home/data/abb_nyc_housing_2019_data.csv')
    abb_df.loc[abb_df['reviews_per_month'].isnull(), 'reviews_per_month'] = 0
    abb_df = abb_df.loc[:, ~ abb_df.columns.str.contains('^Unnamed')]
    abb_df2 = abb_df.drop(columns=['id', 'name', 'host_name', 'last_review'])
    abb_df2 = abb_df2.set_index('host_id')
    abb_df2 = abb_df2.rename(columns={"neighbourhood_group": "borough"})

    abb_df2.to_csv('/Users/nli/dev/airflow_home/data/abb_nyc_housing_2019_data2.csv')


t1c = PythonOperator(
    task_id='abb_data_etl',
    python_callable=abb_data_etl,
    provide_context=False,
    dag=dag,
)


def abb_csv_to_mysql():
    conn = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/airflow_project')
    df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_shooting_data2.csv', delimiter=',')
    df.to_sql(name='nyc_shooting', con=conn, schema='airflow_project', if_exists='replace')


t1d = PythonOperator(
        task_id='csv_to_mysql',
        python_callable=abb_csv_to_mysql,
        dag=dag,
)

t1e1 = PostgresOperator(
    task_id='create_table_postgres_abb_nyc_data',
    postgres_conn_id='postgres_nyc_data',
    sql='''CREATE TABLE IF NOT EXISTS nyc_data.nyc_abb(
            host_id integer,
            borough varchar(255),
            neighbourhood varchar(255),
            latitude float,
            longitude float,
            room_type varchar(255),
            price integer,
            minimum_nights integer,
            number_of_reviews integer,	
            reviews_per_month float,
            calculated_host_listings_count integer,
            availability_365 integer);
            ''',
    dag=dag,
)


# path = '/Users/nli/dev/airflow_home/data/abb_nyc_housing_2019_data2.csv'
# t1e2 = PostgresOperator(
#     task_id='import_to_postgres',
#     postgres_conn_id='postgres_nyc_data',
#     sql=f"DELETE FROM nyc_data.nyc_abb; COPY nyc_data.nyc_abb FROM '{path}' DELIMITER ',' CSV HEADER;",
#     dag=dag,
# )


###############################################################
#NYC_Park_Data


def get_park_data():
    """
    NYC park information dataset based on 2019
    """
    nyc_park_url = 'https://data.cityofnewyork.us/api/views/ghu2-eden/rows.csv?accessType=DOWNLOAD'
    response = requests.get(nyc_park_url)
    with open('/Users/nli/dev/airflow_home/data/nyc_park_data.csv', 'wb') as file:
        file.write(response.content)


t2a = PythonOperator(
    task_id='get_nyc_park_data',
    python_callable=get_park_data,
    provide_context=False,
    dag=dag,
)


def nyc_park_data_etl():
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


t2b = PythonOperator(
    task_id='etl_nyc_park',
    python_callable=nyc_park_data_etl,
    provide_context=False,
    dag=dag,
)


def nyc_park_csv_to_mysql():
    conn = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/airflow_project')
    df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_park_data2.csv', delimiter=',')
    df.to_sql(name='nyc_park', con=conn, schema='airflow_project', if_exists='replace')


t2c = PythonOperator(
        task_id='csv_to_mysql',
        python_callable=nyc_park_csv_to_mysql,
        dag=dag
)


t2d1 = PostgresOperator(
    task_id='create_table_nyc_park',
    postgres_conn_id='postgres_nyc_data',
    sql='''CREATE TABLE IF NOT EXISTS nyc_data.nyc_park(
            borough varchar(255),
            acres float,
            typecatego varchar(255),
            waterfront varchar(255));
            ''',
    dag=dag,


)

path = '/Users/nli/dev/airflow_home/data/nyc_park_data2.csv'
t2d2 = PostgresOperator(
    task_id='import_to_postgres',
    postgres_conn_id = 'postgres_nyc_data',
    sql=f"DELETE FROM nyc_data.nyc_park; COPY nyc_data.nyc_park FROM '{path}' DELIMITER ',' CSV HEADER;",
    dag=dag,
)


###############################################################
#NYC_Shooting_Data


def get_shooting_data():
    """
    NYC shooting cases dataset 2019
    """
    nyc_shooting_url = 'https://data.cityofnewyork.us/api/views/5ucz-vwe8/rows.csv?accessType=DOWNLOAD&api_foundry=true'
    response = requests.get(nyc_shooting_url)
    with open('/Users/nli/dev/airflow_home/data/nyc_shooting_data.csv', 'wb') as file:
        file.write(response.content)


t3a = PythonOperator(
    task_id='get_nyc_shooting_data',
    python_callable=get_shooting_data,
    provide_context=False,
    dag=dag,
)


def nyc_shooting_data_etl():
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


t3b = PythonOperator(
    task_id='etl_nyc_shooting',
    python_callable=nyc_shooting_data_etl,
    provide_context=False,
    dag=dag,
)


def nyc_shooting_csv_to_mysql():
    conn = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/airflow_project')
    df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_shooting_data2.csv', delimiter=',')
    df.to_sql(name='nyc_shooting', con=conn, schema='airflow_project', if_exists='replace')


t3c = PythonOperator(
        task_id='csv_to_mysql',
        python_callable=nyc_shooting_csv_to_mysql,
        dag=dag
)


t3d1 = PostgresOperator(
    task_id='create_table_nyc_shooting',
    postgres_conn_id='postgres_nyc_data',
    sql='''CREATE TABLE IF NOT EXISTS nyc_data.nyc_shooting(
            occur_date date, 
            occur_time time, 
            borough varchar(255) , 
            precinct integer, 
            vic_sex varchar(255),
            latitude float, 
            longitude float); 
            ''',
    dag=dag,
)


path = '/Users/nli/dev/airflow_home/data/nyc_shooting_data2.csv'
t3d2 = PostgresOperator(
    task_id = 'import_to_postgres',
    postgres_conn_id = 'postgres_nyc_data',
    sql=f"DELETE FROM nyc_data.nyc_shooting; COPY nyc_data.nyc_shooting FROM '{path}' DELIMITER ',' CSV HEADER;",
    dag=dag,
)


###############################################################
#NYC_hotel_Data

def get_hotel_data():
    """
    NYC hotel information dataset based on 2019
    """
    nyc_hotel_url = 'https://data.cityofnewyork.us/api/views/tjus-cn27/rows.csv?accessType=DOWNLOAD&api_foundry=true'
    response = requests.get(nyc_hotel_url)
    with open('/Users/nli/dev/airflow_home/data/nyc_hotel_data.csv', 'wb') as file:
        file.write(response.content)


t4a = PythonOperator(
    task_id='get_nyc_hotel_data',
    python_callable=get_hotel_data,
    provide_context=False,
    dag=dag,
)


def nyc_hotel_data_etl():
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
    nyc_ht_df.to_html('/Users/nli/dev/airflow_home/data/nyc_hotel_data2.html')


t4b = PythonOperator(
    task_id='etl_nyc_hotel',
    python_callable=nyc_hotel_data_etl,
    provide_context=False,
    dag=dag,
)


def nyc_hotel_csv_to_mysql():
    conn = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/airflow_project')
    df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_hotel_data2.csv', delimiter=',')
    df.to_sql(name='nyc_hotel', con=conn, schema='airflow_project', if_exists='replace')


t4c = PythonOperator(
        task_id='csv_to_mysql',
        python_callable=nyc_hotel_csv_to_mysql,
        dag=dag,
)


t4d1 = PostgresOperator(
    task_id='create_table_postgres_nyc_hotel',
    postgres_conn_id='postgres_nyc_data',
    sql='''CREATE TABLE IF NOT EXISTS nyc_data.nyc_hotel(
            postcode integer,
            borough varchar(255),
            latitude float,
            longitude float);
            ''',
    dag=dag,
)


path = '/Users/nli/dev/airflow_home/data/nyc_hotel_data2.csv'
t4d2 = PostgresOperator(
    task_id='import_to_postgres',
    postgres_conn_id='postgres_nyc_data',
    sql=f"DELETE FROM nyc_data.nyc_hotel; COPY nyc_data.nyc_hotel FROM '{path}' DELIMITER ',' CSV HEADER;",
    dag=dag,
)


# def get_jupyter():
#     pm.execute_notebook('/Users/nli/dev/airflow_home/nyc_hotel_data.ipynb',
#                         '/Users/nli/dev/airflow_home/nyc_hotel_data_output.ipynb',
#                         parameters={'file_name': '/Users/nli/dev/airflow_home/data/nyc_hotel_data2.csv'}
#                         )
#
#
# t6 = PythonOperator(
#     task_id='call_jupyter_report',
#     provide_context=False,
#     python_callable=get_jupyter,
#     dag=dag,
# )


###############################################################
#NYC_hotel_Data


def get_housing_data():
    """
    NYC public housing location dataset
    """
    nyc_housing_url = 'https://data.cityofnewyork.us/api/views/hg8x-zxpr/rows.csv?accessType=DOWNLOAD'
    response = requests.get(nyc_housing_url)
    with open('/Users/nli/dev/airflow_home/data/nyc_housing_data.csv', 'wb') as file:
        file.write(response.content)


t5a = PythonOperator(
    task_id = 'get_nyc_housing_data',
    python_callable=get_housing_data,
    provide_context=False,
    dag=dag,
)


def nyc_housing_data_etl():
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
    nyc_h_df = nyc_h_df.set_index('project_id')
    nyc_h_df.to_csv('/Users/nli/dev/airflow_home/data/nyc_housing_data2.csv')


t5b = PythonOperator(
    task_id='etl_nyc_housing',
    python_callable=nyc_housing_data_etl,
    provide_context=False,
    dag=dag,
)


def nyc_housing_csv_to_mysql():
    conn = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/airflow_project')
    df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_housing_data2.csv', delimiter=',')
    df.to_sql(name='nyc_housing', con=conn, schema='airflow_project', if_exists='replace')


t5c = PythonOperator(
        task_id='csv_to_mysql',
        python_callable=nyc_housing_csv_to_mysql,
        dag=dag
)


t5d1 = PostgresOperator(
    task_id='create_table_nyc_pb_housing',
    postgres_conn_id='postgres_nyc_data',
    sql='''CREATE TABLE IF NOT EXISTS nyc_data.nyc_pb_housing(
            project_id integer, 
            borough varchar(255), 
            extremely_low_income_units integer , 
            very_low_income_units integer ,
            low_income_units integer , 
            moderate_income_units integer , 
            middle_income_units integer ,
            other_income_units integer , 
            total_units integer);
            ''',
    dag=dag,
)


path = '/Users/nli/dev/airflow_home/data/nyc_housing_data2.csv'
t5d2 = PostgresOperator(
    task_id='import_to_postgres',
    postgres_conn_id='postgres_nyc_data',
    sql=f"DELETE FROM nyc_data.nyc_pb_housing; COPY nyc_data.nyc_pb_housing FROM '{path}' DELIMITER ',' CSV HEADER;",
    dag=dag,
)


###############################################################
#NYC_hotel_Data


def get_hot_spot_data():
    """
    NYC free hot spot information dataset based on 2019
    """
    nyc_hot_spot_url = 'https://data.cityofnewyork.us/api/views/varh-9tsp/rows.csv?accessType=DOWNLOAD'
    response = requests.get(nyc_hot_spot_url)
    with open('/Users/nli/dev/airflow_home/data/nyc_hot_spot_data.csv', 'wb') as file:
        file.write(response.content)


t6a = PythonOperator(
    task_id='get_nyc_hot_spot_data',
    python_callable=get_hot_spot_data,
    provide_context=False,
    dag=dag,
)


def nyc_hot_spot_data_etl():
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


t6b = PythonOperator(
    task_id='etl_nyc_hot_spot',
    python_callable=nyc_hot_spot_data_etl,
    provide_context=False,
    dag=dag,
)


def nyc_hot_spot_csv_to_mysql():
    conn = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/airflow_project')
    df = pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_hot_spot_data2.csv', delimiter=',')
    df.to_sql(name='nyc_hot_spot', con=conn, schema='airflow_project', if_exists='replace')


t6c = PythonOperator(
        task_id='csv_to_mysql',
        python_callable=nyc_hot_spot_csv_to_mysql,
        dag=dag
)


t6d1 = PostgresOperator(
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
t6d2 = PostgresOperator(
    task_id='import_to_postgres',
    postgres_conn_id='postgres_nyc_data',
    sql=f"DELETE FROM nyc_data.nyc_hot_spot; COPY nyc_data.nyc_hot_spot FROM '{path}' DELIMITER ',' CSV HEADER;",
    dag=dag,
)


t_final = DummyOperator(
    task_id='data_etl_completed',
    retries=1,
    dag=dag
)

#DAGS Flowing Chart
t0 >> t1a >> t1b >> t1c >> t1d >> t_final,
t0 >> t1a >> t1b >> t1c >> t1e1  >> t_final,
t0 >> t2a >> t2b >> t2c >> t_final,
t0 >> t2a >> t2b >> t2d1 >> t2d2 >> t_final,
t0 >> t3a >> t3b >> t3c >> t_final,
t0 >> t3a >> t3b >> t3d1 >> t3d2 >> t_final,
t0 >> t4a >> t4b >> t4c >> t_final,
t0 >> t4a >> t4b >> t4d1 >> t4d2 >> t_final,
t0 >> t5a >> t5b >> t5c >> t_final,
t0 >> t5a >> t5b >> t5d1 >> t5d2 >> t_final,
t0 >> t6a >> t6b >> t6c >> t_final,
t0 >> t6a >> t6b >> t6d1 >> t6d2 >> t_final,


