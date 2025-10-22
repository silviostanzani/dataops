from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
 
def extrair_dados():
    # Simula extração de dados
    print("Extraindo dados da fonte...")
    return { compras: 100, artefatos: 100 }
 
def transformar_dados(**context):
    dados = context.ti.xcom_pull(task_ids='extrair')

    # Processa os dados
    soma_dados = dados.compras + dados.artefatos

    print(f'Soma: {soma_dados}')
    return dados
 
with DAG(
    'workflow_luis',
    default_args={'retries': 2},
    start_date=datetime(2025, 10, 21),
    schedule_interval='@daily',
    catchup=False
) as dag:
 
    t1 = PythonOperator(task_id='extrair', python_callable=extrair_dados)
    t2 = PythonOperator(task_id='transformar', python_callable=transformar_dados)
 
    t1 >> t2