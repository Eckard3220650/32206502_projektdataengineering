from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime
import os

def import_data():
    os.system("python C:/Users/49176/batch_basierte_Datenarchitektur/Daten Importieren.py")

with DAG('data_import_dag', start_date=datetime(2024, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    download_data = PythonOperator(
        task_id='download_data',
        python_callable=import_data
    )
