from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def gerar_dados():
    # Gera um dicionário de dados simulados
    print("Gerando dados de compras e artefatos...")
    return {'compras': 100, 'artefatos': 100}

def somar_dados(**context):
    dados = context['ti'].xcom_pull(task_ids='gerar')
    soma = dados['compras'] + dados['artefatos']
    print(f"Soma dos parâmetros: {soma}")

with DAG(
    'kaique',
    default_args={'retries': 1},
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Executa manualmente
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='gerar',
        python_callable=gerar_dados
    )

    t2 = PythonOperator(
        task_id='somar',
        python_callable=somar_dados
    )

    t1 >> t2
