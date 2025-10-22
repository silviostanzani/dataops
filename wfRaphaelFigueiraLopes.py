from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def gerar_documento():

    return {'compras': 100, 'artefatos': 100}

def somar_itens(**context):
    doc = context['ti'].xcom_pull(task_ids='gerar_documento')
    total = doc['compras'] + doc['artefatos']
    print(f"Soma de compras + artefatos = {total}")

with DAG(
    'RaphaelFigueiraLopes_thinking',
    default_args={'retries': 0},
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='gerar_documento',
        python_callable=gerar_documento
    )

    t2 = PythonOperator(
        task_id='somar_itens',
        python_callable=somar_itens
    )

    t1 >> t2
