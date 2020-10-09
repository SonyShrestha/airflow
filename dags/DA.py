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
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

#importing custom modules
from utilities.utilities import *
from utilities.variables import *


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
    description='A simple tutorial DAG',
    schedule_interval=CRON_EXPRESSION_DA,
    template_searchpath=[SQL_SCRIPT_LOCATION],
    catchup=False
    )   


setting_session = BashOperator(
    task_id='setting_session',
    bash_command=PYTHON_LOCATION + ' ' +
    os.path.join(MONITOR_SCRIPT_LOCATION, 'main.py -c 1 1 1 1 -t set_session '),
    dag=dag,
    #on_success_callback=notify_email_success, 
    on_failure_callback=notify_email_failure, 
    params={
        "email_list":EMAIL_LIST,
        "task_success_msg":'Session id successfully created.',
        'task_failure_msg':'Failed to create session id.'
        
    }, 
    trigger_rule = 'all_success'
)

get_raw_data_result = BashOperator(
    task_id='get_raw_data_result',
    bash_command=PYTHON_LOCATION + ' ' +
    os.path.join(MONITOR_SCRIPT_LOCATION, 'main.py -c 1 1 1 1 -t get_raw_data_result '),
    dag=dag,
    #on_success_callback=notify_email_success,
    on_failure_callback=notify_email_failure,
    params={
        "email_list":EMAIL_LIST,
        "task_success_msg":'Result for validation of raw data generated.',
        'task_failure_msg':'Failed to generate result for raw data validation.'

    },
    trigger_rule = 'all_success'
)


clientend_control_total = BashOperator(
    task_id='clientend_control_total',
    bash_command=PYTHON_LOCATION + ' ' +
    os.path.join(MONITOR_SCRIPT_LOCATION, 'main.py -c 1 1 1 1 -t clientend_control_total '),
    dag=dag,
    #on_success_callback=notify_email_success,
    on_failure_callback=notify_email_failure,
    params={
        "email_list":EMAIL_LIST,
        "task_success_msg":'Session id successfully created.',
        'task_failure_msg':'Failed to create session id.'

    },
    trigger_rule = 'all_success'
)

'''raw_data_migrate = BashOperator(
    task_id='raw_data_migrate',
    bash_command=PYTHON_LOCATION + ' ' +
    os.path.join(MONITOR_SCRIPT_LOCATION, 'main.py -c 1 1 1 1 -t data_migration'),
    dag=dag, 
    #on_success_callback=notify_email_success,
    on_failure_callback=notify_email_failure, 
    params={
        "email_list":EMAIL_LIST,
        "task_success_msg":'Raw data successfully migrated from client end to our database.',
        "task failur_msg":'Failed to migrate raw data from client end to our database.'
    }, 
    trigger_rule = 'all_success'
)'''

raw_data_validate = BashOperator(
    task_id='raw_data_validate',
    bash_command=PYTHON_LOCATION + ' ' +
    os.path.join(MONITOR_SCRIPT_LOCATION, 'main.py -c 1 1 1 1 -t validation'),
    dag=dag, 
    #on_success_callback=notify_email_success,
    on_failure_callback=notify_email_failure, 
    params={
        "email_list":EMAIL_LIST,
        "file": [os.path.join(MONITOR_SCRIPT_LOCATION, 'data/validation/validation_1_1_1_1.csv')],
        "task_success_msg":'Validation of raw data has been successfully completed.',
        "task_failure_msg":'Failed to validate raw data.'
    }, 
    trigger_rule = 'all_success'
)

'''control_total_migrate = BashOperator(
    task_id='control_total_migrate',
    bash_command=PYTHON_LOCATION + ' ' +
    os.path.join(MONITOR_SCRIPT_LOCATION, 'main.py -c 1 1 1 1 -t control_total_migrate'),
    dag=dag,
    #on_success_callback=notify_email_success, 
    on_failure_callback=notify_email_failure, 
    params={
        "email_list":EMAIL_LIST,
        "task_success_msg":'Control total has been successfully migrated from client end to our database.',
        "task_failure_msg":'Failed to migrate control total from client end to our database.'
    }, 
    trigger_rule = 'all_success'
)'''

run_sp = BashOperator(
    task_id='run_sp',
    bash_command=PYTHON_LOCATION + ' ' +
    os.path.join(MONITOR_SCRIPT_LOCATION, 'main.py -c 1 1 1 2 -t run_sp '),
    dag=dag, 
    #on_success_callback=notify_email_success,
    on_failure_callback=notify_email_failure, 
    params={
        "email_list":EMAIL_LIST,
        "task_success_msg":'Stored procedure to create table for validation of source data has been successfully called.',
        "task_failure_msg":'Failed to call stored proceure to create table for validation of source data.'
        
    }, 
    trigger_rule = 'all_success'
)

validation = BashOperator(
    task_id='validation',
    bash_command=PYTHON_LOCATION + ' ' +
    os.path.join(MONITOR_SCRIPT_LOCATION, 'main.py -c 1 1 1 1 -t validation '),
    dag=dag, 
    #on_success_callback=notify_email_success
    on_failure_callback=notify_email_failure, 
    params={
        "email_list":EMAIL_LIST,
        "file": [os.path.join(MONITOR_SCRIPT_LOCATION, 'data/validation/validation_1_1_1_2.csv')],
        "task_success_msg":"Validation of source data has been succefully created.",
        "task_failure_msg":"Failed to validate source data."
    }, 
    trigger_rule = 'all_success'
)




DA_script_etl = BashOperator(
    task_id='DA_script_etl',
    bash_command=PYTHON_LOCATION + ' '+os.path.join(DA_SCRIPT_LOCATION, 'main.py etl'),
    dag=dag, 
    #on_success_callback=notify_email_success,
    on_failure_callback=notify_email_failure,
    params={
        "file": [],
        "email_list":EMAIL_LIST,
        "task_success_msg":"Raw data has been successfully loaded from client side view to our database.",
        "task_failure_msg":"Failed to load raw data from client side view to our database."
    }, 
    trigger_rule='all_success', 
    #on_success_callback=notify_email_success
)


DA_script_find_salary = BashOperator(
    task_id='find_salary',
    bash_command=PYTHON_LOCATION + ' '+os.path.join(DA_SCRIPT_LOCATION, 'main.py find-salary'),
    dag=dag, 
    #on_success_callback=notify_email_success,
    on_failure_callback=notify_email_failure, 
    params={
        "file": [],
        "email_list":EMAIL_LIST,
        "task_success_msg":"Salary identification algorithm has been successfully executed.",
        "task_failure_msg":"Failed to execute salary identification algorithm."
    }, 
    trigger_rule='all_success', 
    )

DA_script_filter_accounts = BashOperator(
    task_id='filter_accounts',
    bash_command=PYTHON_LOCATION + ' '+os.path.join(DA_SCRIPT_LOCATION, 'main.py filter-accounts'),
    dag=dag,
    #on_success_callback=notify_email_success,
    on_failure_callback=notify_email_failure,
    params={
        "file": [],
        "email_list":EMAIL_LIST,
        "task_success_msg":"Filter account module successfully executed.",
        "task_failure_msg":"Failed to filter accounts."
    },
    trigger_rule='all_success',
    )

DA_script_calculate_fact = BashOperator(
    task_id='calculate_fact',
    bash_command=PYTHON_LOCATION + ' '+os.path.join(DA_SCRIPT_LOCATION, 'main.py calculate-fact -c no'),
    dag=dag, 
    on_failure_callback=notify_email_failure, 
    params={
        "file": [],
        "email_list":EMAIL_LIST,
        "task_success_msg":"DA fact calculation module has been successfully executed.",
        "task_failure_msg":"Failed to execute DA fact calculation module."
    }, 
    trigger_rule='all_success', 
    #on_success_callback=notify_email_success
    )


DA_script_calculate_score = BashOperator(
    task_id='calculate_score',
    bash_command=PYTHON_LOCATION + ' '+os.path.join(DA_SCRIPT_LOCATION, 'main.py calculate-score'),
    dag=dag, 
    on_failure_callback=notify_email_failure, 
    params={
        "file": [],
        "email_list":EMAIL_LIST,
        "task_success_msg":"DA score calculation module has been successfully executed.",
        "task_failure_msg":"Failed to execute DA score calculation module."
    }, 
    trigger_rule='all_success', 
    #on_success_callback=notify_email_success
    )


DA_calculate_metrics_fact = BashOperator(
    task_id='calculate_metrics_fact',
    bash_command=PYTHON_LOCATION + ' '+os.path.join(DA_SCRIPT_LOCATION, 'main.py calculate-metrics-fact'),
    dag=dag, 
    on_failure_callback=notify_email_failure, 
    params={
        "file": [],
        "email_list":EMAIL_LIST,
        "task_success_msg":"DA metrics fact calculation module has been successfully executed.",
        "task_failure_msg":"Failed to execute DA metrics fact calculation module."
    }, 
    trigger_rule='all_done', 
    #on_success_callback=notify_email_success
    )

DA_calculate_segments = BashOperator(
    task_id='calculate_segments',
    bash_command=PYTHON_LOCATION + ' '+os.path.join(DA_SCRIPT_LOCATION, 'main.py calculate-segments'),
    dag=dag, 
    on_failure_callback=notify_email_failure, 
    params={
        "file": [],
        "email_list":EMAIL_LIST,
        "task_success_msg":"DA segment calculation module has been successfully executed.",
        "task_failure_msg":"Failed to execute DA segment calculation module."
    }, 
    trigger_rule='all_success', 
    # on_success_callback=notify_email_success
    )

DA_monitor = BashOperator(
    task_id='monitor',
    bash_command=PYTHON_LOCATION + ' '+os.path.join(DA_SCRIPT_LOCATION, 'main.py monitor'),
    dag=dag, 
    on_failure_callback=notify_email_failure, 
    params={
        "file": [],
        "email_list":EMAIL_LIST,
        "task_success_msg":"DA monitoring result has been successfully generated.",
        "task_failure_msg":"Failed to generate DA monitoring Result."
    }, 
    trigger_rule='all_success', 
    #on_success_callback=notify_email_success
    )

send_monitor_reports = BashOperator(
    task_id='send_monitor_reports',
    bash_command=PYTHON_LOCATION + ' '+os.path.join(MONITOR_SCRIPT_LOCATION, 'report_generation.py'),
    dag=dag, 
    on_failure_callback=notify_email_failure, 
    params={
        "file": [os.path.join(MONITOR_SCRIPT_LOCATION,'data','reports.zip')],
        "email_list":EMAIL_LIST,
        "task_success_msg":"Pleas find the attached DA monitoring result.",
        "task_failure_msg":"Failed to send DA monitoring result."
    }, 
    trigger_rule='all_success', 
    on_success_callback=notify_email_success
    )



fc_data_analysis=MySqlOperator(
    task_id='fc_data_analysis',
    mysql_conn_id="mysql_server",
    on_failure_callback=notify_email_failure,
    params={
    "raw_db_name":raw_db_name,
    "master_db_name":master_db_name,
    "processing_db_name":processing_db_name,
    "email_list":EMAIL_LIST,
    "task_success_msg":"Sql Script for DA result analysis has been successfully executed.",
    "task failure_msg":"Failed to execute SQL Script for DA result analysis."
    },
    sql="fc_data_analysis.sql",
    dag=dag
)


'''run_sp_fc_processing_to_history = BashOperator(
    task_id='run_sp_fc_processing_to_history',
    bash_command=PYTHON_LOCATION + ' ' +
    os.path.join(MIGRATE_DB_SCRIPT_LOCATION, 'main.py run_sp_fc_processing_to_history '),
    dag=dag,
    on_failure_callback=notify_email_failure,
    params={
        "email_list":EMAIL_LIST,
        "task_success_msg":"Tables successfully migrated from processing database to history database.",
        "task_failure_msg":"Failed to migrate tables from processing database to history database."
    },
    trigger_rule = 'all_success'
)'''

email_status = EmailOperator(
    task_id = 'email_status',
    to=EMAIL_LIST,
    trigger_rule='all_success',
    subject='Execution Completed',
    html_content="The Decision Analytics has completed it's execution",
    dag=dag
)

setting_session>>get_raw_data_result>>raw_data_validate>>clientend_control_total>>DA_script_etl>>run_sp>>validation>>DA_script_find_salary>>DA_script_filter_accounts>>DA_calculate_metrics_fact>>DA_script_calculate_fact>>DA_calculate_segments>>DA_script_calculate_score>>DA_monitor>> send_monitor_reports>>fc_data_analysis>>email_status

