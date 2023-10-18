from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


def test_azure_blob_connection():

        hook = WasbHook()
        container_properties = hook.create_container('test11')
        print("Connection to Azure Blob Storage successful!")
        print("Container Properties:", container_properties)



dag = DAG(
    dag_id='test_azure_blob_connection',
    start_date=datetime(2023, 7, 12),
    schedule_interval=None
)

test_connection_task = PythonOperator(
    task_id='test_connection_task',
    python_callable=test_azure_blob_connection,
    dag=dag
)

test_connection_task