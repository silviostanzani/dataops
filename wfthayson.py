from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def gerar_documento():
    # Simula a geração de um documento
    print("Gerando documento...")
    return {'compras': 100, 'artefatos': 100}

def somar_valores(**context):
    dados = context['ti'].xcom_pull(task_ids='gerar_documento')
    soma = dados['compras'] + dados['artefatos']
    print(f"Soma dos valores: {soma}")

with DAG(
    'thayson',
    default_args={'retries': 1},
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='gerar_documento',
        python_callable=gerar_documento
    )

    t2 = PythonOperator(
        task_id='somar_valores',
        python_callable=somar_valores
    )

    t1 >> t2
