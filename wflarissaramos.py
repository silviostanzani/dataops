from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def gerar_documento():
    doc = {'compras': 100, 'artefatos': 100}
    print(f"Documento gerado: {doc}")
    return doc

def somar_parametros(**context):
    ti = context['ti']
    dados = ti.xcom_pull(task_ids='gerar_documento')
    if not dados or not all(k in dados for k in ('compras', 'artefatos')):
        raise ValueError("Chaves 'compras' e/ou 'artefatos' ausentes no documento.")
    soma = dados['compras'] + dados['artefatos']
    print(f"Soma (compras + artefatos): {soma}")

with DAG(
    'larissa-ramos',
    default_args={'retries': 1},
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['exemplo', 'airflow', 'xcom']
) as dag:

    t1 = PythonOperator(
        task_id='gerar_documento',
        python_callable=gerar_documento
    )

    t2 = PythonOperator(
        task_id='somar_parametros',
        python_callable=somar_parametros
    )

    t1 >> t2

