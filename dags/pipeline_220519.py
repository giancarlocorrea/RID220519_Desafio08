from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# RID #220519
default_args = {
    'owner': 'giancarlo_220519',
    'start_date': datetime(2026, 3, 27)
}

def carga_bronze():
    path_origem = '/opt/airflow/data/raw_data.csv'
    path_destino = '/opt/airflow/data/bronze'
    
    # Cria a pasta se não existir
    os.makedirs(path_destino, exist_ok=True)
    
    df = pd.read_csv(path_origem)
    df.to_csv(f'{path_destino}/raw_data.csv', index=False)
    print("Dados movidos para a camada Bronze com sucesso.")

with DAG('pipeline_desafio_220519', 
         default_args=default_args, 
         schedule_interval=None, 
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='ingestao_bronze',
        python_callable=carga_bronze
    )