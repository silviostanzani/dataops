from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def gerar_documento():
    return {'compras': 100, 'artefatos': 100}

def somar_itens(**context):
    documento = context['ti'].xcom_pull(task_ids='t1')
    total = documento['compras'] + documento['artefatos']
    print(f"Soma de compras + artefatos = {total}")

with DAG(
    dag_id='rafaelyokoyama_workflow',
    description='DAG com duas tarefas: gerar documento e somar valores',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='t1',
        python_callable=gerar_documento
    )

    t2 = PythonOperator(
        task_id='t2',
        python_callable=somar_itens
    )

    t1 >> t2
