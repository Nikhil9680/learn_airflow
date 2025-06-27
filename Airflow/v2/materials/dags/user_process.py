from airflow import DAG
from datetime import datetime
import json
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from pandas import json_normalize


# More robust error handling example
def process_response(response):
    try:
        return json.loads(response.text)
    except json.JSONDecodeError:
        logging.error(f"Failed to parse response: {response.text}")
        raise

def _process_user(ti):
    user=ti.xcom_pull(task_ids="extract_data")
    user=user['results'][0]
    processed_user=json_normalize({
        'firstname':user['name']['first'],
        'lastname':user['name']['last'],
        'country':user['location']['country'],
        'username':user['login']['username'],
        'password':user['login']['password'],
        'email':user['email']
    })
    processed_user.to_csv('/tmp/processed_user_new.csv', index=None, header=False)


def _store_user():
    hook=PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users_new FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user_new.csv'
    )


with DAG(dag_id="user_process",start_date=datetime(2023,1,1,),
         tags=["nikhil-learn"],
         schedule_interval="@daily",catchup=False):

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id='postgres',
        sql=""" 
        CREATE TABLE IF NOT EXISTS users_new
        (
            firstname TEXT NOT NULL,
            lastname TEXT NOT NULL,
            country TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            email TEXT NOT NULL
        );
        """
    )

    is_api_available=HttpSensor(
        task_id="is_api_available",
        http_conn_id="user_api",
        endpoint="api/",
        poke_interval=60,
        timeout=60
    )

    extract_data = HttpOperator(
    task_id="extract_data",
    http_conn_id="user_api",
    endpoint="api/",
    method='GET',
    response_filter=process_response,
    log_response=True,
    headers={'Accept': 'application/json'}
    )

    process_user=PythonOperator(
    task_id="process_user",
    python_callable=_process_user
    )

    store_user=PythonOperator(
    task_id="store_user",
    python_callable=_store_user
    )

create_table >> is_api_available >> extract_data >> process_user >> store_user