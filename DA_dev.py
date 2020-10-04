"""
    Author      : Siddhi
    Created Date:2020-07-14
    Descripion  : Dag program for DA.
"""

#importing default modules
import os
import shutil
import pendulum
from datetime import timedelta, datetime
import pandas as pd

#importing airflow modules
from airflow import DAG
from airflow.utils import timezone
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.hooks.webhdfs_hook import WebHDFSHook
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook

#importing custom modules
from airflow_dag_template.utilities.utilities import *
from airflow_dag_template.utilities.variables import *


def load_to_hdfs(source, dest, hdfs_conn_id = 'webhdfs_default',):
    hook = WebHDFSHook(hdfs_conn_id)
    print('###############################################################################################')
    print(hook.get_conn().status('/user/siddhi'))
    print('###############################################################################################')


def data_dump(table, tmp_file, mysql_conn_id = 'default_mysql'):
    if os.path.exists(tmp_file):
        os.remove(tmp_file)
    mysql_hook = MySqlHook(mysql_conn_id = mysql_conn_id)
    mysql_hook.bulk_dump(table, tmp_file)

def transform(tmp_file):
    df = pd.read_csv(tmp_file, delimiter='\t',header=None)
    df.iloc[:,1] = 'XXX'
    df.columns = ['id','name']
    print(30*'*')
    print(df)
    print(30*'*')
    df.to_csv(tmp_file,index = False,)

def upload_db(table, tmp_file,mysql_conn_id = 'default_mysql'):
    df = pd.read_csv(tmp_file,)
    
    mysql_hook = MySqlHook(mysql_conn_id = mysql_conn_id)
    print(df)
    print('###############################################################################################')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('truncate {}'.format(table))
    conn.commit()
    print('###############################################################################################')
    df.to_sql(table, mysql_hook.get_sqlalchemy_engine(), if_exists = 'append', index = False)

local_tz = pendulum.timezone('Asia/Kathmandu')
start_date = datetime(**START_DATE, tzinfo=local_tz)

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'email': EMAIL_LIST,
    'email_on_failure': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_done'
}
dag = DAG(
    'template',
    default_args=default_args,
    description='Template Dag.',
    schedule_interval=CRON_EXPRESSION,
    catchup = False,
)

file_sense = FileSensor(
    task_id = 'file_check',
    filepath = '/home/siddhi/tst/source_file.csv', dag = dag
)

copy = BashOperator(
    task_id = 'copy_file',
    bash_command = 'cp /home/siddhi/tst/source_file.csv /home/siddhi/dest/dest_file.csv', 
    dag = dag)

dummy = DummyOperator(
    task_id = 'Transformation-Operations',
    dag=dag
)


# upload_to_hdfs = PythonOperator(
#     task_id='upload',
#     python_callable=load_to_hdfs,
#     op_kwargs={
#         'source': '/home/siddhi/airflow/dags/airflow_dag_template/source/source_file.csv',
#         'dest': str('hdfs://localhost:50070/test/source_file.csv'),
#     },
#     dag=dag)


data_dump = PythonOperator(
    task_id='data_dump',
    python_callable=data_dump,
    op_kwargs={
        'table': 'test',
        'tmp_file': '/home/siddhi/tst/source_file.csv',
        'mysql_conn_id':'local_mysql'
    },
    dag=dag)


transform_op = PythonOperator(
    task_id = 'data_transform',
    python_callable=transform,
    op_kwargs={
        'tmp_file':'/home/siddhi/tst/source_file.csv'
    },
    dag = dag
)
email = EmailOperator(
    task_id = 'email',
    to=['siddhi.47.skb@gmail.com'],
    subject = 'test',
    dag = dag,
    html_content='Hello'
)

upload_to_db = PythonOperator(
    task_id = 'upload_to_db',
    python_callable=upload_db,
    op_kwargs={
        'tmp_file':'/home/siddhi/tst/source_file.csv',
        'table':'target_table',
        'mysql_conn_id':'local_mysql'
    },
    dag = dag
)
data_dump>>file_sense>>dummy>>transform_op>>[copy,upload_to_db]>>email
