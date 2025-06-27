from airflow import DAG
from airflow.operators.bash import BashOperator
from groups.group_download import download_tasks
from groups.group_transform import transform_tasks


from datetime import datetime

with DAG('group_dag_with_taskgroups', start_date=datetime(2022, 1, 1),
         tags=["nikhil-learn"],
    schedule_interval='@daily', catchup=False) as dag:

    args = {'start_date': dag.start_date, 'schedule_interval':dag.schedule_interval, 'tags':dag.tags, 'catchup':dag.catchup}
    
    downloads = download_tasks()

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transform = transform_tasks()

    downloads >> check_files >> transform
