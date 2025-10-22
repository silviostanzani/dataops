from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def gerar_documento():
    print("Gerando documento...")
    return {'compras':100, 'artefatos':100}
 
def somar_dados(**context):
    print("Somando dados...")
    dados = context['ti'].xcom_pull(task_ids='gerar_documento')
    dados['total'] = dados['compras'] + dados['artefatos']
    return dados

with DAG(
    'etl_jeff',
    default_args={'retries': 2},
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='gerar_documento', python_callable=gerar_documento)
    t2 = PythonOperator(task_id='somar_dados', python_callable=somar_dados)
 
    t1 >> t2
