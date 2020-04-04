from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
# from airflow.utils.dates import days_ago
# from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta
import json
import requests
import pprint
import os


default_args = {
    'owner': 'Norton_Li2',
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
    # category_alis = 'steak'

    # app_id = 'client_id'
    # app_secret = 'client_secret'
    # data = {'grant_type': "client_credentials",
    #         'client_id': app_id,
    #         'client_secret': app_secret}
    # token = requests.post('http://api.', data = data)
    # access_token = token.json('access_token')

    my_api_key = os.environ.get('yelp_token')
    
    url = 'https://api.yelp.com/v3/businesses/search'
    headers = {'Authorization': 'bearer %s' % my_api_key}

    params = {'location': 'New_York_City',
              'term': 'steak_house',
              'price': '3',
              }

    response = requests.get(url=url, params=params, headers=headers)
    # pprint.pprint(response.json()['business'])

    #convert response to a Json String
    business_data = response.json()
    print(json.dumps(business_data, indent=3))


t4 = PythonOperator(
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