# from airflow import DAG
# from datetime import datetime
# import json
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.postgres_operator import PostgresOperator
# from airflow.providers.http.sensors.http import HttpSensor
# from airflow.providers.http.operators.http import SimpleHttpOperator
# from airflow.operators.python import PythonOperator
# from airflow.hooks.postgres_hook import PostgresHook


# from pandas import json_normalize



# def _process_user(ti):
#     user=ti.xcom_pull(task_ids="extract_data")
#     user=user['results'][0]
#     processed_user=json_normalize({
#         'firstname':user['name']['first'],
#         'lastname':user['name']['last'],
#         'country':user['location']['country'],
#         'username':user['login']['username'],
#         'password':user['login']['password'],
#         'email':user['email']
#     })
#     processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)


# def _store_user():
#     hook=PostgresHook(postgres_conn_id='postgres')
#     hook.copy_expert(
#         sql="COPY users FROM stdin WITH DELIMITER as ','",
#         filename='/tmp/processed_user.csv'
#     )


# with DAG(dag_id="user_processing",start_date=datetime(2023,1,1,),
#          schedule_interval="@daily",catchup=False):
    
#     create_table = PostgresOperator(
#         task_id="create_table",
#         postgres_conn_id='postgres',
#         sql=""" 
#         CREATE TABLE IF NOT EXISTS users
#         (
#             firstname TEXT NOT NULL,
#             lastname TEXT NOT NULL,
#             country TEXT NOT NULL,
#             username TEXT NOT NULL,
#             password TEXT NOT NULL,
#             email TEXT NOT NULL
#         );
#         """
#     )

#     is_api_available=HttpSensor(
#         task_id="is_api_available",
#         http_conn_id="user_api",
#         endpoint="api/",
#         poke_interval=60,
#         timeout=60
#     )

#     extract_data=SimpleHttpOperator(
#         task_id="extract_data",
#         http_conn_id="user_api",
#         endpoint="api/",
#         method='GET',
#         response_filter=lambda response : json.loads(response.text),
#         log_response=True
#     )

#     process_user=PythonOperator(
#         task_id="process_user",
#         python_callable=_process_user
#     )

#     store_user=PythonOperator(
#         task_id="store_user",
#         python_callable=_store_user
#     )


# create_table >> is_api_available >> extract_data >> process_user >> store_user