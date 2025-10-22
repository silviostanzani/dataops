from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def gerar_documento():
    return {'compras': 100, 'artefatos': 100}

def somar_parametros(**context):
    documento = context['ti'].xcom_pull(task_ids='gerar_doc')
    soma = documento['compras'] + documento['artefatos']
    print(soma)

with DAG(
    'lucas_nogueira_wf',
    default_args={'retries': 2},
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    tarefa_gerar_documento = PythonOperator(
        task_id='gerar_doc',
        python_callable=gerar_documento
    )

    tarefa_somar_parametros = PythonOperator(
        task_id='somar_params',
        python_callable=somar_parametros
    )

    tarefa_gerar_documento >> tarefa_somar_parametros
