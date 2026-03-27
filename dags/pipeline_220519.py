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
    """Lê o arquivo bruto e garante a cópia na camada Bronze."""
    path_origem = '/opt/airflow/data/raw_data.csv'
    path_destino = '/opt/airflow/data/bronze'
    
    if not os.path.exists(path_origem):
        raise FileNotFoundError(f"Arquivo bruto não encontrado em: {path_origem}")
        
    os.makedirs(path_destino, exist_ok=True)
    df = pd.read_csv(path_origem)
    df.to_csv(f'{path_destino}/raw_data.csv', index=False)
    print("Camada Bronze: Ingestão concluída com sucesso.")

def processamento_silver():
    """Limpa dados, trata nomes de colunas e calcula idade precisa."""
    path_bronze = '/opt/airflow/data/bronze/raw_data.csv'
    path_silver = '/opt/airflow/data/silver'
    os.makedirs(path_silver, exist_ok=True)

    # Detecta automaticamente se o separador é vírgula ou ponto e vírgula
    df = pd.read_csv(path_bronze, sep=None, engine='python')
    
    # Padronização de colunas (Garante 'nome', 'email' e 'data_nascimento')
    df.columns = [
        c.strip().lower()
        .replace(' ', '_')
        .replace('á', 'a')
        .replace('ç', 'c') 
        for c in df.columns
    ]
    
    # Filtra apenas linhas com dados essenciais presentes
    colunas_obrigatorias = ['nome', 'email', 'data_nascimento']
    df.dropna(subset=[c for c in colunas_obrigatorias if c in df.columns], inplace=True)
    
    # Validação simples de e-mail
    if 'email' in df.columns:
        df = df[df['email'].str.contains('@', na=False)]
    
    # Processamento de Data e Idade
    if 'data_nascimento' in df.columns:
        df['data_nascimento'] = pd.to_datetime(df['data_nascimento'], errors='coerce')
        df.dropna(subset=['data_nascimento'], inplace=True)
        
        # Lógica de idade precisa para 2026
        hoje = datetime(2026, 3, 27)
        df['idade'] = df['data_nascimento'].apply(
            lambda x: hoje.year - x.year - ((hoje.month, hoje.day) < (x.month, x.day))
        )
    
    df.to_csv(f'{path_silver}/silver_data.csv', index=False)
    print("Camada Silver: Dados limpos e idade calculada com precisão.")

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
    
    # Fluxo atual: Bronze -> Silver
    t1 >> t2