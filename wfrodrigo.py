# criar um workflow com duas tarefas,
#   a primeira deve gerar um documento com o seguinte formato {'compras':100, 'artefatos':100}
#   a segunda deve fazer o primt da soma dos parametros compras e artefatos

# fazer upload da solução no github, para ser executada no airflow

# coloque o seu nome no script

# troque a string etl_simples pelo seu nome

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
 
def extrair_dados():
    # Simula extração de dados
    print("Extraindo dados da fonte...")
    return {'compras':100, 'artefatos':100}
 
def transformar_dados(**context):
    dados = context['ti'].xcom_pull(task_ids='extrair')
    # Processa os dados
    dados['total'] = dados['compras'] + dados['artefatos']
    return dados

with DAG(
    'rodrigo',
    default_args={'retries': 2},
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
 
    t1 = PythonOperator(task_id='extrair', python_callable=extrair_dados)
    t2 = PythonOperator(task_id='transformar', python_callable=transformar_dados)
 
    t1 >> t2
