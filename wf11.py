from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def tarefa1():
    # Simula extração de dados
    print("Extraindo dados da fonte...")
    return {'tools': 5, 'cars': 10}

def tarefa2(**context):
    dados = context['ti'].xcom_pull(task_ids='extrair')
    # Processa os dados
    dados['total'] = dados['tools'] + dados['cars']
    print('dados')  
    print(dados)

with DAG(
    'silvio',
    default_args={'retries': 2},
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='extrair', python_callable=tarefa1)
    t2 = PythonOperator(task_id='transformar', python_callable=tarefa2)
    

    t1 >> t2
