from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def gerar_documento():
    doc = {'compras': 100, 'artefatos': 100}
    print(f"Documento gerado: {doc}")
    return doc  

def somar_campos(**context):
    ti = context['ti']
    doc = ti.xcom_pull(task_ids='gerar_documento')
    if not doc:
        raise ValueError("Documento não encontrado no XCom.")
    total = doc['compras'] + doc['artefatos']
    print(f"Soma de compras + artefatos = {total}")

with DAG(
    'henrique_marques',  
    default_args={'retries': 2},
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='gerar_documento',
        python_callable=gerar_documento
    )

    t2 = PythonOperator(
        task_id='somar_campos',
        python_callable=somar_campos
    )

    t1 >> t2

