from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import pandas as pd
from datetime import timedelta
from airflow import DAG
from sqlalchemy import create_engine
import requests
from datetime import datetime
import os


mysqluser = os.environ.get('mysql_user')
mysqlkey = os.environ.get('mysql_key')
postgreuser = os.environ.get('postgres_user')
postgreskey = os.environ.get('postgres_key')


default_args = {
    'owner': 'Norton_Li',
    'start_date': datetime.now(),
    #'start_date': datetime.days_ago(2),
    'email': ['nortonlyr@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    dag_id='airbnb_nyc_housing_correlation_study_updated6',
    default_args=default_args,
    #chedule_interval=timedelta(days=1),
    description='Airbnb NYC housing open data and NYC open data of 2019 study',
    )


t0 = DummyOperator(
    task_id='nyc_data_etl_start_here',
    retries=1,
    dag=dag
)


path_abb = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/abb_nyc_housing_2019_data.csv')
path_etl_abb = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/abb_nyc_housing_2019_data2.csv')

def get_nyc_abb_data():
    """
    get the nyc airbnb data
    """
    url = 'https://s3.amazonaws.com/norton.bucket/airflow_project/AB_NYC_2019.csv'
    response = requests.get(url)
    with open(path_abb, 'wb') as f:
        f.write(response.content)


t1a = PythonOperator(
    task_id = 'get_nyc_abb_data',
    python_callable=get_nyc_abb_data,
    provide_context=False,
    dag=dag,
)


def abb_data_etl():
    """
    Data cleaning with download file and drop some unused columns
    """
    abb_df = pd.read_csv(path_abb)

    abb_df2 = abb_df.drop(columns=['id', 'name', 'host_name', 'last_review'])
    abb_df2 = abb_df2.rename(columns={"neighbourhood_group": "borough"})
    abb_df2.to_csv(path_etl_abb)


t1b = PythonOperator(
    task_id='abb_data_etl',
    python_callable=abb_data_etl,
    provide_context=False,
    dag=dag,
)


def abb_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/airflow_project")
    df = pd.read_csv(path_etl_abb, delimiter=',')
    df.to_sql(name='nyc_abb', con=conn, schema='airflow_project', if_exists='replace')


t1c = PythonOperator(
        task_id='abb_to_mysql',
        python_callable=abb_csv_to_mysql,
        dag=dag,
)


def abb_csv_to_postgres():
    conn = create_engine("postgresql+psycopg2://" + postgreuser + ":" + postgreskey + "@localhost:5432/airflow_p")
    df = pd.read_csv(path_etl_abb, delimiter=',')
    df.to_sql(name='nyc_airbb', con=conn, schema='nyc_data', if_exists='replace')


t1d = PythonOperator(
    task_id='abb_to_postgres',
    python_callable=abb_csv_to_postgres,
    dag=dag,
)

path_abb_nb = os.path.join(os.path.dirname(os.path.dirname(__file__)),'jupyter_nb/nyc_abb_vis.ipynb')
t1e = BashOperator(
    task_id='vis_w_jupyter_nb',
    bash_command='jupyter nbconvert --to notebook --execute {}'.format(path_abb_nb)
                   + ' --output nyc_abb_vis{{ ds }}.ipynb',
    dag=dag,
)


###############################################################
#NYC_Park_Data
path_park = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/nyc_park_data.csv')
path_etl_park = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/nyc_park_data2.csv')

def get_park_data():
    """
    NYC park information dataset based on 2019
    """
    nyc_park_url = 'https://data.cityofnewyork.us/api/views/ghu2-eden/rows.csv?accessType=DOWNLOAD'
    response = requests.get(nyc_park_url)
    with open(path_park, 'wb') as file:
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
    nyc_ht_df = pd.read_csv(path_park)
    nyc_ht_df = nyc_ht_df.drop(columns=['the_geom', 'GISPROPNUM', 'OBJECTID', 'OMPPROPID', 'DEPARTMENT',
       'PERMITDIST', 'PERMITPARE', 'PARENTID', 'LOCATION','COMMUNITYB','COUNCILDIS', 'PRECINCT', 'ZIPCODE','RETIRED',
       'EAPPLY', 'PIP_RATABL', 'GISOBJID', 'CLASS', 'COMMISSION', 'ACQUISITIO', 'ADDRESS', 'JURISDICTI', 'MAPPED',
        'NAME311', 'PERMIT', 'SIGNNAME','SUBCATEGOR', 'URL','NYS_ASSEMB','NYS_SENATE', 'US_CONGRES', 'GlobalID'])
    nyc_ht_df.columns = ['borough', 'acres', 'typecatego', 'waterfront']
    nyc_ht_df = nyc_ht_df.set_index('borough')
    nyc_ht_df.to_csv(path_etl_park)


t2b = PythonOperator(
    task_id='etl_nyc_park',
    python_callable=nyc_park_data_etl,
    provide_context=False,
    dag=dag,
)


def nyc_park_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/airflow_project")
    df = pd.read_csv(path_etl_park, delimiter=',')
    df.to_sql(name='nyc_park', con=conn, schema='airflow_project', if_exists='replace')


t2c = PythonOperator(
        task_id='park_to_mysql',
        python_callable=nyc_park_csv_to_mysql,
        dag=dag
)


def nyc_park_csv_to_postgres():
    conn = create_engine("postgresql+psycopg2://" + postgreuser + ":" + postgreskey + "@localhost:5432/airflow_p")
    df = pd.read_csv(path_etl_park, delimiter=',')
    df.to_sql(name='nyc_park', con=conn, schema='nyc_data', if_exists='replace')


t2d = PythonOperator(
    task_id='park_to_postgres',
    python_callable=nyc_park_csv_to_postgres,
    dag=dag,
)


###############################################################
#NYC_Shooting_Data
path_shooting = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/nyc_shooting_data.csv')
path_etl_shooting = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data/nyc_shooting_data2.csv')

def get_shooting_data():
    """
    NYC shooting cases dataset 2019
    """
    nyc_shooting_url = 'https://data.cityofnewyork.us/api/views/5ucz-vwe8/rows.csv?accessType=DOWNLOAD&api_foundry=true'
    response = requests.get(nyc_shooting_url)
    with open(path_shooting, 'wb') as file:
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
    nyc_s_df = pd.read_csv(path_shooting)
    nyc_s_df = nyc_s_df.drop(columns=['INCIDENT_KEY', 'JURISDICTION_CODE', 'LOCATION_DESC',
                                                'STATISTICAL_MURDER_FLAG', 'PERP_AGE_GROUP', 'PERP_SEX', 'PERP_RACE',
                                                'VIC_AGE_GROUP', 'VIC_AGE_GROUP', 'VIC_RACE',
                                                 'X_COORD_CD', 'Y_COORD_CD'])
    nyc_s_df.columns = ['occur_date', 'occur_time', 'borough', 'precinct', 'vic_sex','latitude', 'longitude']
    nyc_s_df = nyc_s_df.set_index('occur_date')
    nyc_s_df.to_csv(path_etl_shooting)


t3b = PythonOperator(
    task_id='etl_nyc_shooting',
    python_callable=nyc_shooting_data_etl,
    provide_context=False,
    dag=dag,
)


def nyc_shooting_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey +"@localhost:3306/airflow_project")
    df = pd.read_csv(path_etl_shooting, delimiter=',')
    df.to_sql(name='nyc_shooting', con=conn, schema='airflow_project', if_exists='replace')


t3c = PythonOperator(
        task_id='shooting_to_mysql',
        python_callable=nyc_shooting_csv_to_mysql,
        dag=dag
)


def nyc_shooting_csv_to_postgres():
    conn = create_engine("postgresql+psycopg2://" + postgreuser + ":" + postgreskey + "@localhost:5432/airflow_p")
    df = pd.read_csv(path_etl_shooting, delimiter=',')
    df.to_sql(name='nyc_shooting', con=conn, schema='nyc_data', if_exists='replace')


t3d = PythonOperator(
    task_id='shooting_to_postgres',
    python_callable=nyc_shooting_csv_to_postgres,
    dag=dag,
)


###############################################################
#NYC_hotel_Data
path_hotel = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data/nyc_hotel_data.csv')
path_etl_hotel = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data/nyc_hotel_data2.csv')

def get_hotel_data():
    """
    NYC hotel information dataset based on 2019
    """
    nyc_hotel_url = 'https://data.cityofnewyork.us/api/views/tjus-cn27/rows.csv?accessType=DOWNLOAD&api_foundry=true'
    response = requests.get(nyc_hotel_url)
    with open(path_hotel, 'wb') as file:
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

    nyc_ht_df = pd.read_csv(path_hotel)
    nyc_ht_df = nyc_ht_df.drop(columns=['PARID', 'BOROCODE', 'BLOCK', 'LOT', 'TAXYEAR', 'STREET NUMBER',
                                          'STREET NAME', 'BLDG_CLASS', 'TAXCLASS', 'OWNER_NAME', 'Community Board',
                                          'Council District', 'Census Tract', 'BIN', 'BBL', 'NTA'])
    nyc_ht_df.columns = ['postcode', 'borough', 'latitude', 'longitude']
    nyc_ht_df = nyc_ht_df.set_index('postcode')
    nyc_ht_df.to_csv(path_etl_hotel)


t4b = PythonOperator(
    task_id='etl_nyc_hotel',
    python_callable=nyc_hotel_data_etl,
    provide_context=False,
    dag=dag,
)


def nyc_hotel_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/airflow_project")
    df = pd.read_csv(path_etl_hotel, delimiter=',')
    df.to_sql(name='nyc_hotel', con=conn, schema='airflow_project', if_exists='replace')


t4c = PythonOperator(
        task_id='hotel_to_mysql',
        python_callable=nyc_hotel_csv_to_mysql,
        dag=dag,
)


def nyc_hotel_csv_to_postgres():
    conn = create_engine("postgresql+psycopg2://" + postgreuser + ":" + postgreskey + "@localhost:5432/airflow_p")
    df = pd.read_csv(path_etl_hotel, delimiter=',')
    df.to_sql(name='nyc_hotel', con=conn, schema='nyc_data', if_exists='replace')


t4d = PythonOperator(
    task_id='hotel_to_postgres',
    python_callable=nyc_hotel_csv_to_postgres,
    dag=dag,
)


###############################################################
#NYC_public_housing_Data
path_housing = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data/nyc_housing_data.csv')
path_etl_housing = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data/nyc_housing_data2.csv')

def get_housing_data():
    """
    NYC public housing location dataset
    """
    nyc_housing_url = 'https://data.cityofnewyork.us/api/views/hg8x-zxpr/rows.csv?accessType=DOWNLOAD'
    response = requests.get(nyc_housing_url)
    with open(path_housing, 'wb') as file:
        file.write(response.content)


t5a = PythonOperator(
    task_id='get_nyc_housing_data',
    python_callable=get_housing_data,
    provide_context=False,
    dag=dag,
)


def nyc_housing_data_etl():
    """
    Data cleaning with download file and drop some unused columns
    """
    nyc_h_df = pd.read_csv(path_housing)
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
    nyc_h_df.to_csv(path_etl_housing)


t5b = PythonOperator(
    task_id='etl_nyc_housing',
    python_callable=nyc_housing_data_etl,
    provide_context=False,
    dag=dag,
)


def nyc_housing_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/airflow_project")
    df = pd.read_csv(path_etl_housing, delimiter=',')
    df.to_sql(name='nyc_housing', con=conn, schema='airflow_project', if_exists='replace')


t5c = PythonOperator(
        task_id='housing_to_mysql',
        python_callable=nyc_housing_csv_to_mysql,
        dag=dag
)


def nyc_pb_housing_csv_to_postgres():
    conn = create_engine("postgresql+psycopg2://" + postgreuser + ":" + postgreskey + "@localhost:5432/airflow_p")
    df = pd.read_csv(path_etl_housing, delimiter=',')
    df.to_sql(name='nyc_pb_housing', con=conn, schema='nyc_data', if_exists='replace')


t5d = PythonOperator(
    task_id='housing_to_postgres',
    python_callable=nyc_pb_housing_csv_to_postgres,
    dag=dag,
)


###############################################################
#NYC_hotel_Data
path_hotspot = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data/nyc_hot_spot.csv')
path_etl_hotspot = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data/nyc_hot_spot2.csv')

def get_hot_spot_data():
    """
    NYC free hot spot information dataset based on 2019
    """
    nyc_hot_spot_url = 'https://data.cityofnewyork.us/api/views/varh-9tsp/rows.csv?accessType=DOWNLOAD'
    response = requests.get(nyc_hot_spot_url)
    with open(path_hotspot, 'wb') as file:
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
    nyc_hs_df = pd.read_csv(path_hotspot)
    nyc_hs_df = nyc_hs_df.drop(columns=['BORO', 'the_geom', 'OBJECTID', 'NAME', 'LOCATION',
        'X', 'Y', 'LOCATION_T', 'REMARKS', 'SSID','SOURCEID', 'NTACODE', 'NTANAME', 'COUNDIST',
        'POSTCODE', 'BOROCD', 'CT2010', 'BOROCT2010', 'BIN', 'BBL', 'DOITT_ID'])
    nyc_hs_df.columns = ['type', 'provider','latitude', 'longitude', 'city', 'activated', 'borough_code','borough' ]
    nyc_hs_df = nyc_hs_df.set_index('type')
    nyc_hs_df.to_csv(path_etl_hotspot)


t6b = PythonOperator(
    task_id='etl_nyc_hot_spot',
    python_callable=nyc_hot_spot_data_etl,
    provide_context=False,
    dag=dag,
)


def nyc_hot_spot_csv_to_mysql():
    conn = create_engine("mysql+pymysql://" + mysqluser + ":" + mysqlkey + "@localhost:3306/airflow_project")
    df = pd.read_csv(path_etl_hotspot, delimiter=',')
    df.to_sql(name='nyc_hot_spot', con=conn, schema='airflow_project', if_exists='replace')


t6c = PythonOperator(
        task_id='hot_spot_to_mysql',
        python_callable=nyc_hot_spot_csv_to_mysql,
        dag=dag
)


def nyc_hot_spot_csv_to_postgres():
    conn = create_engine("postgresql+psycopg2://" + postgreuser + ":" + postgreskey + "@localhost:5432/airflow_p")
    df = pd.read_csv(path_etl_hotspot, delimiter=',')
    df.to_sql(name='nyc_hot_spot', con=conn, schema='nyc_data', if_exists='replace')


t6d = PythonOperator(
    task_id='hot_spot_to_postgres',
    python_callable=nyc_hot_spot_csv_to_postgres,
    dag=dag,
)

t_final = DummyOperator(
    task_id='data_etl_completed',
    retries=1,
    dag=dag
)


#DAGS Flowing Chart
t0 >> t1a >> t1b >> t1c >> t_final >> t1e,
t0 >> t1a >> t1b >> t1d >> t_final,
t0 >> t2a >> t2b >> t2c >> t_final,
t0 >> t2a >> t2b >> t2d >> t_final,
t0 >> t3a >> t3b >> t3c >> t_final,
t0 >> t3a >> t3b >> t3d >> t_final,
t0 >> t4a >> t4b >> t4c >> t_final,
t0 >> t4a >> t4b >> t4d >> t_final,
t0 >> t5a >> t5b >> t5c >> t_final,
t0 >> t5a >> t5b >> t5d >> t_final,
t0 >> t6a >> t6b >> t6c >> t_final,
t0 >> t6a >> t6b >> t6d >> t_final,


