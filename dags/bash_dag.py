from datetime import datetime
import time
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_hello():
    time.sleep(1000)
    return 'Hi world from first Airflow DAG!'


dag = DAG('hi', description='Hi World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hi_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

hi_operator
