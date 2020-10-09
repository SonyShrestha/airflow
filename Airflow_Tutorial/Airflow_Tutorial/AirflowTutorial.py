"""
Author: Sony Shrestha
Description: Understanding DAG Program in detail
Date: 9 August, 2020
"""
#importing default modules
import os
import shutil
import pendulum

#to import dag (compulsory to be imported for each dag program)
from airflow import DAG

# these imports can be useful
from airflow.utils import timezone
from airflow.utils.dates import days_ago

#import datetime 
from datetime import datetime, timedelta

#import operators (as per our requirement)
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

# import python functions
from Airflow_Tutorial.python_files.main import *

# import cutom defined modules to make some variables config driven
from Airflow_Tutorial.utilities.utilities import *
from Airflow_Tutorial.utilities.variables import *


local_tz = pendulum.timezone('Asia/Kathmandu')
start_date = datetime(**START_DATE, tzinfo=local_tz)


# define default arguments
default_args={
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
    # 'retry_delay':timedelta(secconds=5)
}

dag=DAG(
    'AirflowTutorial',
    default_args=default_args,
    description='Understanding DAG Program',
    schedule_interval='@daily',
    template_searchpath=['/root/airflow/dags/Airflow_Tutorial/sql_files'],
    catchup=False ) 

# Understanding BashOperator
t1 = BashOperator(
task_id='BashOperatorExample',
bash_command='echo "Hello World"',
dag=dag,
#on_success_callback=notify_email_success,
#on_failure_callback=notify_email_failure,
params={
        "email_list":EMAIL_LIST,
        "success_message":"Task completed successfully",
        "failure_message":"Task Failed"
},
trigger_rule='all_done'
)


t2 = PythonOperator(
task_id='PythonOperatorExample',
python_callable=python_function,
dag=dag,
#on_success_callback=notify_email_success,
#on_failure_callback=notify_email_failure,
params={
        "email_list":EMAIL_LIST,
        "success_message":"Task completed successfully",
        "failure_message":"Task Failed"
},
trigger_rule='all_done'
)


t3 = MySqlOperator(
task_id='MySqlOperatorExample',
mysql_conn_id="server_55",
sql="create_table.sql",
on_success_callback=notify_email_success,
on_failure_callback=notify_email_failure,
params={
  "master":master,
  #"success_message":"Mysql Operator passed successfully",
  #"failure_message":"Mysql Operator did not pass successfully",
  #"success_file":['/root/airflow/dags/Airflow_Tutorial/sql_files/create_table.sql'],
  #"failure_file":['/root/airflow/dags/Airflow_Tutorial/sql_files/create_table.py'],
  "email_list":EMAIL_LIST
  },
dag=dag
)


t4 = EmailOperator(
task_id='EmailOperatorExample',
subject='Test: Email Operator',
cc='sony.sth8@gmail.com',
bcc='pop.son4p@gmail.com',
to='sony.shrestha@extensodata.com',
html_content=""" <h1> Email Operator Passed </h1> <br> <br> Email sent successfully <br> """,
files=['/root/airflow/dags/Airflow_Tutorial/sql_files/create_table.sql'],
dag=dag,
#on_success_callback=notify_email_success,
#on_failure_callback=notify_email_failure,
params={
        "email_list":EMAIL_LIST,
        "success_message":"Task completed successfully",
        "failure_message":"Task Failed"
},
trigger_rule='all_done'
)

t5 = DummyOperator(
task_id='DummyOperatorExample',
dag=dag)

t6=BranchPythonOperator(
task_id='BranchPythonOperatorExample',
python_callable=decide_branch,
dag=dag,
trigger_rule='all_done'
)

t7=BashOperator(
task_id='on_success',
bash_command='echo "SUCCESS"',
dag=dag)

t8=BashOperator(
task_id='on_failure',
bash_command='echo "FAILURE"',
dag=dag)


[t1,t2] >> t5 >>[t3,t4] >> t6 >> [t7,t8]              
