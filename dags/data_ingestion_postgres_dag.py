from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime
import os

def ingest_to_postgres():
    os.system("python C:/Users/49176/batch_basierte_Datenarchitektur/Daten in PostgreSQL.py")

with DAG('data_ingestion_postgres_dag', start_date=datetime(2024, 1, 1), schedule_interval='@hourly', catchup=False) as dag:
    postgres_ingestion = PythonOperator(
        task_id='postgres_ingestion',
        python_callable=ingest_to_postgres
    )
