from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
 
def gerar_dados():
    # Simula extração de dados
    print("Extraindo dados da fonte...")
    return {'compras':100, 'artefatos':100}
 
def mostrar_dados(**context):
    dados = context['ti'].xcom_pull(task_ids='extrair')
    # Processa os dados
    dados['total'] = dados['compras'] + dados['artefatos']
    return dados
    
 
with DAG(
    'Maury_Viana',
    default_args={'retries': 2},
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
 
    t1 = PythonOperator(task_id='GerarDoc', python_callable=gerar_dados)
    t2 = PythonOperator(task_id='Mostrar', python_callable=mostrar_dados)

 
    t1 >> t2 
