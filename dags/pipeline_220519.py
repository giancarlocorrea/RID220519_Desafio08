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

# --- 1. DEFINIÇÕES DAS FUNÇÕES ---

def carga_bronze():
    path_origem = '/opt/airflow/data/raw_data.csv'
    path_destino = '/opt/airflow/data/bronze'
    
    if not os.path.exists(path_origem):
        raise FileNotFoundError(f"Arquivo não encontrado em: {path_origem}")
        
    os.makedirs(path_destino, exist_ok=True)
    df = pd.read_csv(path_origem)
    df.to_csv(f'{path_destino}/raw_data.csv', index=False)
    print("Camada Bronze: Sucesso")

def processamento_silver():
    path_bronze = '/opt/airflow/data/bronze/raw_data.csv'
    path_silver = '/opt/airflow/data/silver'
    os.makedirs(path_silver, exist_ok=True)

    # Carregamento com tratamento
    df = pd.read_csv(path_bronze)
    
    # Limpeza de nulos e validação de e-mail
    df.dropna(subset=['nome', 'email', 'data_nascimento'], inplace=True)
    df = df[df['email'].str.contains('@', na=False)]
    
    # Conversão de data com 'coerce' (transforma erros em NaT para não quebrar o código)
    df['data_nascimento'] = pd.to_datetime(df['data_nascimento'], errors='coerce')
    df.dropna(subset=['data_nascimento'], inplace=True)
    
    # Lógica de idade precisa
    hoje = datetime(2026, 3, 27)
    df['idade'] = df['data_nascimento'].apply(
        lambda x: hoje.year - x.year - ((hoje.month, hoje.day) < (x.month, x.day))
    )
    
    df.to_csv(f'{path_silver}/silver_data.csv', index=False)
    print("Camada Silver: Sucesso")

# --- 2. DEFINIÇÃO DA DAG ---

with DAG('pipeline_desafio_220519', 
         default_args=default_args, 
         schedule_interval=None, 
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='ingestao_bronze',
        python_callable=carga_bronze
    )
    
    t2 = PythonOperator(
        task_id='limpeza_silver',
        python_callable=processamento_silver
    )    
    
    t1 >> t2