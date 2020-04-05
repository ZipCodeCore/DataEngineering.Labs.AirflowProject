from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago
from datetime import timedelta
import json
import requests
import os
import pandas as pd


default_args = {
    'owner': 'Norton_Li_2',
    'start_date': datetime.now(),
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='yelp_api_data_collection',
    default_args=default_args,
    description='Yelp API data',
)


def get_yelp_api():
    file_counter = 0
    offset_counter = 1
    my_api_key = os.environ('yelp_token')

    while file_counter <= 100:
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


def read_json(file_path):
    with open(file_path, 'r') as f:
        jason_input = json.load(f)
    return jason_input

def read_all_json_files(JSON_ROOT):
    result = []
    for dirpath, dirname, filenames in os.walk(JSON_ROOT):
        for f in filenames:
            if f.endswith('.json'):
                json_content2 = read_json(os.path.join(JSON_ROOT, f))
                for i in json_content2['businesses']:
                    i['source'] = f
                    result.append(i)
    df_location = pd.DataFrame(result)
    return df_location

yelp_restautant_df = read_all_json_files('/Users/nli/dev/airflow_home/data')
# print(yelp_restautant_df)

# with open('/Users/nli/dev/airflow_home/data/pickle_data/yelp_pickle', 'wb') as handler:
#     pickle.dump(yelp_restautant_df, handler)

yelp_restautant_df.to_pickle('/Users/nli/dev/airflow_home/data/pickle_data/yelp_restaurant.pickle')
yelp_restautant_df.to_csv('/Users/nli/dev/airflow_home/data/pickle_data/yelp_restaurants.csv')


t1 = PythonOperator(
    task_id='get_yelp_data',
    python_callable=get_yelp_api,
    provide_context=False,
    dag=dag,
)