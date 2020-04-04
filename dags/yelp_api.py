from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta
import json
import requests
import os


default_args = {
    'owner': 'Norton_Li_2',
    'start_date': datetime.now(),
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='get_yelp_api_data',
    default_args=default_args,
    description='Yelp API data',
)


def get_yelp_api():

    file_counter = 0
    offset_counter = 1
    my_api_key = os.environ('yelp_token')

    while file_counter <= 50:
        name = 'yelp_restaurants_' + str(file_counter)
        url = 'https://api.yelp.com/v3/businesses/search'
        headers = {'Authorization': 'bearer %s' % my_api_key}
        params = {'location': 'New_York_City',
                'term': 'michelin',
                'price': '3, 4',
                'limit': 50,
                'offset': 50,
              }

        response = requests.get(url=url, params=params, headers=headers)
        business_data = response.json()

        with open('/Users/nli/dev/airflow_home/data/' + (name + '.json'), 'w') as outfile:
            json.dump(business_data, outfile)
        file_counter += 1
        offset_counter += 50



t1 = PythonOperator(
    task_id='get_yelp_data',
    python_callable=get_yelp_api,
    provide_context=False,
    dag=dag,
)



    # file_counter = 0
    # offset_counter = 1
    # token = os.environ.get('yelp_token')
    #
    # while file_counter <= 39:
    #     name = ("location_" + str(file_counter))
    #     url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/locations?location&limit=1000&offset=' + str(offset_counter)
    #     header = {"Token": token, 'Content-Type': 'application/json'}
    #     r = requests.get(url=url, headers=header)
    #     data = r.json()
    #
    #     with open('/Users/nli/dev/PythonFundamentals.Labs.DataAcqusitionLab/location_json/' + \
    #               (name + ".json"), 'w') as outfile:
    #         json.dump(data, outfile)
    #     file_counter += 1
    #     offset_counter += 1000