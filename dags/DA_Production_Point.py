"""
Author: Sony Shrestha
Created Date:2020-07-28
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
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator


from airflow.operators.sensors import SqlSensor


#importing custom modules
from utilities.utilities import *
from utilities.variables import *


from copy import deepcopy


import logging

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
    'DAProductionPoint',
    default_args=default_args,
    description='Migrate database',
    schedule_interval=CRON_EXPRESSION_DA_Production_Point,
    template_searchpath=[SQL_SCRIPT_LOCATION],
    catchup=False
)

def update_check_signal():
    mysql_session=create_session(mysql_url)
    query1="""update {}.md_check_signal set flag=0 where id=1"""
    mysql_session.execute(query1.format(master_db_name))
    mysql_session.commit()
    

'''check_signal =  BashOperator(
    task_id='check_signal',
    bash_command=PYTHON_LOCATION + ' ' +
    os.path.join(MIGRATE_DB_SCRIPT_LOCATION, 'main.py check_signal '),    
    dag=dag, 
    on_failure_callback=notify_email_failure, 
    params={
        "email_list":EMAIL_LIST,
        "task_success_msg":"Checking signal for running MigrateDatabase passed successfully.",
        "task_failure_msg":"Signal not received to run MigrateDatabase."
    },
    trigger_rule = 'all_success'
)'''

check_signal =  SqlSensor(
    task_id='check_signal',
    conn_id='mysql_server',
    sql='SELECT flag FROM '+master_db_name+'.md_check_signal where module="Decision Analytics" and task="Migrate Database"',
    dag=dag,
    timeout=300,
    poke_interval=10,
    soft_fail=True,
    on_failure_callback=notify_email_failure,
    params={
        "email_list":EMAIL_LIST,
        "task_success_msg":"Checking signal for running MigrateDatabase passed successfully.",
        "task_failure_msg":"Signal not received to run MigrateDatabase."
    },
    trigger_rule = 'all_success'
)

run_sp_fc_processing_to_history = BashOperator(
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
)


run_sp_fc_score_to_previous = BashOperator(
    task_id='run_sp_fc_score_to_previous',
    bash_command=PYTHON_LOCATION + ' ' +
    os.path.join(MIGRATE_DB_SCRIPT_LOCATION, 'main.py run_sp_fc_score_to_previous '),
    dag=dag, 
    on_failure_callback=notify_email_failure, 
    params={
        "email_list":EMAIL_LIST,
        "task_success_msg":"Tables successfully migrated from application pointing database to previous database.",
        "task_failure_msg":"Failed to migrate tables from application pointing database to previous database."
    }, 
    trigger_rule = 'all_success'
)

run_sp_fc_processing_to_score = BashOperator(
    task_id='run_sp_fc_processing_to_score',
    bash_command=PYTHON_LOCATION + ' ' +
    os.path.join(MIGRATE_DB_SCRIPT_LOCATION, 'main.py run_sp_fc_processing_to_score '),
    dag=dag,
    on_failure_callback=notify_email_failure,
    params={
        "email_list":EMAIL_LIST,
        "task_success_msg":"Tables successfully migrated from processing database to application pointing database.",
        "task_failure_msg":"Failed to migrate tables from processing database to application pointing database."
    },
    trigger_rule = 'all_success'
)

create_database_bkp = BashOperator(
    task_id='create_database_bkp',
    bash_command= os.path.join(MIGRATE_DB_SCRIPT_LOCATION,'create_bkp.sh '),
    dag=dag,
    on_failure_callback=notify_email_failure,
    on_success_callback=notify_email_success,
    params={
        "email_list":EMAIL_LIST,
        "task_success_msg":"Backup of application pointing database has been successfully created in location "+MIGRATE_DB_SCRIPT_LOCATION+" Backup/",
        "task_failure_msg":"Filed to create backup of application pointing database."
    },
    trigger_rule = 'all_success'
)

update_check_signal=PythonOperator(
    task_id='update_check_signal',
    python_callable=update_check_signal,
    on_failure_callback=notify_email_failure,
    #on_success_callback=notify_email_success,
    params={
         "email_list":EMAIL_LIST,
         "task_success_msg":"Flag set to 0.",
         "task_failure_msg":"Failed to set flag back to 0"
    },
    dag=dag
)

get_cp_past_5_runs=MySqlOperator(
    task_id='get_cp_past_5_runs',
    mysql_conn_id="mysql_server",
    on_failure_callback=notify_email_failure,
    params={
    "master_db_name":master_db_name,
    "app_db_name":app_db_name,
    "email_list":EMAIL_LIST,
    "task_success_msg":"SQL Script to generate score of past five runs successfully execcuted.",
    "task failure_msg":"Failed to execute SQL Script for generating score of past runs."
    },
    sql="cp_past_5_runs.sql",
    dag=dag
)

email_status = EmailOperator(
    task_id = 'email_status',
    to=EMAIL_LIST,
    trigger_rule='all_done',
    subject='Execution Completed',
    html_content="The Decision Analytics has completed it's execution",
    dag=dag
)

check_signal>>run_sp_fc_processing_to_history>>run_sp_fc_score_to_previous>>run_sp_fc_processing_to_score>>create_database_bkp>>get_cp_past_5_runs>>update_check_signal>>email_status
