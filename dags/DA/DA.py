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

#importing airflow modules
from airflow import DAG
from airflow.utils import timezone
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator

#importing custom modules
from DA.utilities.utilities import *
from DA.utilities.variables import *



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
dag = DAG(
    'DecisionAnalytics',
    default_args=default_args,
    description='Development dag for DA.',
    schedule_interval=CRON_EXPRESSION,
    catchup = False,
)



setting_session = BashOperator(
    task_id='setting_session',
    bash_command=PYTHON + ' ' +
    os.path.join(SCRIPT_LOCATION_VALIDATION, 'main.py -c 1 1 1 2 -t set_session '),
    dag=dag, 
    #on_failure_callback=notify_email_failure, 
    params={
        "email_list":EMAIL_LIST
    }, 
    trigger_rule = 'all_success'
)

# Uncomment this code when you need to run the stored procedure in client's side

# run_sp_source = BashOperator(
#     task_id='run_sp_source',
#     bash_command=PYTHON + ' ' +
#     os.path.join(SCRIPT_LOCATION_VALIDATION, 'main.py -c 1 1 1 1 -t run_sp -e client'),
#     dag=dag, 
#     on_failure_callback=notify_email_failure, 
#     params={
#         "email_list":EMAIL_LIST
#     }, 
#     trigger_rule = 'all_success'
# )

source_data_migrate = BashOperator(
    task_id='source_data_migrate',
    bash_command=PYTHON + ' ' +
    os.path.join(SCRIPT_LOCATION_VALIDATION, 'main.py -c 1 1 1 1 -t data_migration'),
    dag=dag, 
    #on_failure_callback=notify_email_failure, 
    params={
        "email_list":EMAIL_LIST
    }, 
    trigger_rule = 'all_success'
)

control_total_migrate = BashOperator(
    task_id='control_total_migrate',
    bash_command=PYTHON + ' ' +
    os.path.join(SCRIPT_LOCATION_VALIDATION, 'main.py -c 1 1 1 1 -t control_total_migrate'),
    dag=dag, 
    #on_failure_callback=notify_email_failure, 
    params={
        "email_list":EMAIL_LIST
    }, 
    trigger_rule = 'all_success'
)

source_data_validate = BashOperator(
    task_id='source_data_validate',
    bash_command=PYTHON + ' ' +
    os.path.join(SCRIPT_LOCATION_VALIDATION, 'main.py -c 1 1 1 1 -t validation'),
    dag=dag, 
    #on_success_callback=notify_email_success,
    #on_failure_callback=notify_email_failure, 
    params={
        "email_list":EMAIL_LIST,
        "file": [os.path.join(SCRIPT_LOCATION_VALIDATION, 'data/validation/validation_1_1_1_1.csv')],
    }, 
    trigger_rule = 'all_success'
)

run_sp_raw = BashOperator(
    task_id='run_sp_raw',
    bash_command=PYTHON + ' ' +
    os.path.join(SCRIPT_LOCATION_VALIDATION, 'main.py -c 1 1 1 2 -t run_sp '),
    dag=dag, 
    #on_failure_callback=notify_email_failure, 
    params={
        "email_list":EMAIL_LIST
    }, 
    trigger_rule = 'all_success'
)

raw_validation = BashOperator(
    task_id='raw_validation',
    bash_command=PYTHON + ' ' +
    os.path.join(SCRIPT_LOCATION_VALIDATION, 'main.py -c 1 1 1 2 -t validation '),
    dag=dag, 
    #on_failure_callback=notify_email_failure, 
    params={
        "email_list":EMAIL_LIST,
        "file": [os.path.join(SCRIPT_LOCATION_VALIDATION, 'data/validation/validation_1_1_1_2.csv')],
    }, 
    trigger_rule = 'all_success'
)


DA_script_etl = BashOperator(
    task_id='DA_script_etl',
    bash_command=PYTHON + ' '+os.path.join(SCRIPT_LOCATION, 'main.py etl'),
    dag=dag, 
    #on_failure_callback=notify_email_failure,
    params={
        "file": [],
        "email_list":EMAIL_LIST
    }, 
    trigger_rule='all_success', 
    #on_success_callback=notify_email_success
)


DA_script_find_salary = BashOperator(
    task_id='find-salary',
    bash_command=PYTHON + ' '+os.path.join(SCRIPT_LOCATION, 'main.py find-salary'),
    dag=dag, 
    #on_failure_callback=notify_email_failure, params={
        # "file": [],
        # "email_list":EMAIL_LIST
    # }, 
    trigger_rule='all_success', 
    #on_success_callback=notify_email_success
    )


DA_script_calculate_fact = BashOperator(
    task_id='calculate-fact',
    bash_command=PYTHON + ' '+os.path.join(SCRIPT_LOCATION, 'main.py calculate-fact -c no'),
    dag=dag, 
    #on_failure_callback=notify_email_failure, params={
        # "file": [],
        # "email_list":EMAIL_LIST
    # }, 
    trigger_rule='all_success', 
    #on_success_callback=notify_email_success
    )


DA_script_calculate_score = BashOperator(
    task_id='calculate-score',
    bash_command=PYTHON + ' '+os.path.join(SCRIPT_LOCATION, 'main.py calculate-score'),
    dag=dag, 
    #on_failure_callback=notify_email_failure, params={
        # "file": [],
        # "email_list":EMAIL_LIST
    # }, 
    trigger_rule='all_success', 
    #on_success_callback=notify_email_success
    )


DA_calculate_metrics_fact = BashOperator(
    task_id='calculate_metrics_fact',
    bash_command=PYTHON + ' '+os.path.join(SCRIPT_LOCATION, 'main.py calculate-metrics-fact'),
    dag=dag, 
    #on_failure_callback=notify_email_failure, params={
        # "file": [],
        # "email_list":EMAIL_LIST
    # }, 
    trigger_rule='all_success', 
    # on_success_callback=notify_email_success
    )

DA_calculate_segments = BashOperator(
    task_id='calculate_segments',
    bash_command=PYTHON + ' '+os.path.join(SCRIPT_LOCATION, 'main.py calculate-segments'),
    dag=dag, 
    #on_failure_callback=notify_email_failure, params={
        # "file": [],
        # "email_list":EMAIL_LIST
    # }, 
    trigger_rule='all_success', 
    # on_success_callback=notify_email_success
    )

DA_monitor = BashOperator(
    task_id='monitor',
    bash_command=PYTHON + ' '+os.path.join(SCRIPT_LOCATION, 'main.py monitor'),
    dag=dag, 
    #on_failure_callback=notify_email_failure, params={
        # "file": [],
        # "email_list":EMAIL_LIST
    # }, 
    trigger_rule='all_success', 
    # on_success_callback=notify_email_success
    )

send_monitor_reports = BashOperator(
    task_id='send_monitor_reports',
    bash_command=PYTHON + ' '+os.path.join(SCRIPT_LOCATION_VALIDATION, 'report_generation.py'),
    dag=dag, 
    #on_failure_callback=notify_email_failure, params={
        # "file": [os.path.join(SCRIPT_LOCATION_VALIDATION,'data','reports.zip')],
        # "email_list":EMAIL_LIST
    # }, 
    trigger_rule='all_success', 
    #on_success_callback=notify_email_success
    )

run_sp_fc_processing_to_history = BashOperator(
    task_id='run_sp_fc_processing_to_history',
    bash_command=PYTHON + ' ' +
    os.path.join(MIGRATE_DB_SCRIPT_LOCATION, 'main.py run_sp_fc_processing_to_history '),
    dag=dag,
    #on_failure_callback=notify_email_failure,
    params={
        "email_list":EMAIL_LIST
    },
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

setting_session>>source_data_migrate>>source_data_validate>>DA_script_etl>>control_total_migrate>>run_sp_raw>>raw_validation>>DA_script_find_salary>>DA_calculate_metrics_fact>>DA_script_calculate_fact>>DA_calculate_segments>>DA_script_calculate_score>>DA_monitor>>send_monitor_reports>>run_sp_fc_processing_to_history>>email_status
