from datetime import datetime
from airflow import DAG
from pendulum import timezone
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators import MultiplyBy5Operator
from operators.multiply_by_5 import MultiplyBy5Operator

def print_hello():
    return 'Hello World'

dag = DAG(
    'crm_m_0004',
    description='crm_m_0004',
    schedule_interval=None,
    start_date= datetime(2020, 1, 1, tzinfo=timezone('Asia/Seoul')),
    catchup=False
)

dummy_operator = DummyOperator(task_id='dummy_task', retries = 3, dag=dag)

for i in range(5):
    hello_operator = PythonOperator(
        task_id="".join(
            ['hello_task', str(i)]),
        python_callable=print_hello, dag=dag)
    dummy_operator >> hello_operator
