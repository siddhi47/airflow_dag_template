"""Created Date:2020-07-28
    Descripion  : Dag program for DA migration.
"""

#importing default modules
import os
import shutil
import pendulum
from datetime import timedelta, datetime

#importing airflow modules
from airflow import DAG
from airflow.utils import timezone
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator

#importing custom modules
from DA_Production_Point.utilities.utilities import *
from DA_Production_Point.utilities.variables import *


from copy import deepcopy

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
    'email_on_retry': False,
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
    # 'trigger_rule': 'all_success'
}
context_param = {
    "success_message":None,
    "failure_message":None,
    "email_list":None,
    "file_list":None
}
dag = DAG(
    'MigrateDatabase',
    default_args=default_args,
    description='Migrate database',
    schedule_interval=CRON_EXPRESSION,
    catchup=False
)


def update_dict(dic, kv):
    new_dict = deepcopy(dic)
    new_dict.update(kv)
    return new_dict


check_signal =  BashOperator(
    task_id='check_signal',
    bash_command=PYTHON + ' ' +
    os.path.join(MIGRATE_DB_SCRIPT_LOCATION, 'main.py check_signal '),
    dag=dag, 
    #on_failure_callback=notify_email_failure, 
    params=update_dict(context_param,{
        "email_list":EMAIL_LIST
    }),
    trigger_rule = 'all_success'
)
"""
run_sp_fc_processing_to_history = BashOperator(
    task_id='run_sp_fc_processing_to_history',
    bash_command=PYTHON + ' ' +
    os.path.join(MIGRATE_DB_SCRIPT_LOCATION, 'main.py run_sp_fc_processing_to_history '),
    dag=dag, 
    #on_failure_callback=notify_email_failure, 
    params=update_dict(context_param,{
        "email_list":EMAIL_LIST
    }),
    trigger_rule = 'all_success'
)
"""

run_sp_fc_score_to_previous = BashOperator(
    task_id='run_sp_fc_score_to_previous',
    bash_command=PYTHON + ' ' +
    os.path.join(MIGRATE_DB_SCRIPT_LOCATION, 'main.py run_sp_fc_score_to_previous '),
    dag=dag, 
    #on_failure_callback=notify_email_failure, 
    params=update_dict(context_param,{
        "email_list":EMAIL_LIST
        
    }), 
    trigger_rule = 'all_success'
)
run_sp_fc_processing_to_score = BashOperator(
    task_id='run_sp_fc_processing_to_score',
    bash_command=PYTHON + ' ' +
    os.path.join(MIGRATE_DB_SCRIPT_LOCATION, 'main.py run_sp_fc_processing_to_score '),
    dag=dag,
    #on_failure_callback=notify_email_failure,
    params=update_dict(context_param,{
	
        "email_list":EMAIL_LIST

    }),
    trigger_rule = 'all_success'
)
create_database_bkp = BashOperator(
    task_id='create_database_bkp',
    bash_command= os.path.join(MIGRATE_DB_SCRIPT_LOCATION,'create_bkp.sh '),
    dag=dag,
    #on_failure_callback=notify_email_failure,
    #on_success_callback=notify_email_success,
    params=update_dict(context_param,{
        "success_message":"Backup File Successfully created in location "+MIGRATE_DB_SCRIPT_LOCATION+"Backup/",
        "email_list":EMAIL_LIST

    }),
    trigger_rule = 'all_success'
)

email_status = EmailOperator(
    task_id = 'email_status',
    to=EMAIL_LIST,
    trigger_rule='all_done',
    subject='Execution Completed',
    html_content="The Decision Analytics has completed it's execution",
    dag=dag
)


check_signal>>run_sp_fc_score_to_previous>>run_sp_fc_processing_to_score>>create_database_bkp>>email_status

