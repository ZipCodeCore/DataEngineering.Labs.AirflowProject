from airflow.operators.python_operator import PythonOperator
import pandas as pd



dag = DAG(
    'csv_request',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

def csv_read(path):
    for full_file in os.listdir(path):
        if full_file.endswith('.json'):
            # full_filename = os.path.join(path, path, full_file)
            full_filename = os.path.join(path, full_file)
            with open(full_filename, 'r') as fi:
                dictionary = json.loads(fi)
                all_dicts.append(dictionary)
        return all_


t1 = PythonOperator(
        task_id='download_file',
        python_callable=download_file_from_ftp,
        provide_context=True,
       	},
  dag=dag)

t1 = DummyOperator()


t1 >> t2
