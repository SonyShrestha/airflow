"""
Author      : Sony Shrestha
    Created Date:2020-09-24
    Descripion  : Dag program for DA.
"""

#importing default modules
import pandas as pd
import os
import shutil
import pendulum
from datetime import timedelta, datetime

#importing airflow modules
from airflow import DAG
from airflow.utils import timezone

from airflow.utils.email import send_email
from sqlalchemy import create_engine

from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator

#importing custom modules
from utilities.utilities import *
from utilities.variables import *
from html_template.email_template import *

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
    'MailMonitorResult',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=CRON_EXPRESSION_Mail_Monitor_Result,
    catchup=False
)


def group_wise_mail():
    mysql_session=create_session(mysql_url)
    get_group_query="""select id from {0}.fc_groups""".format(master_db_name)
    groups=mysql_session.execute(get_group_query)
    group_df=pd.DataFrame(groups.fetchall())
    group_df.columns=groups.keys()
    #print(group_df)
    for index,i in group_df.iterrows():
        get_email_query="""SELECT group_id,group_concat(email) as email_list FROM {0}.fc_users a
        inner join
        {0}.fc_user_group b
        on a.id=b.user_id
        inner join {0}.fc_groups c
        on b.group_id=c.id
        where group_id={1}
        group by group_id
        """.format(master_db_name,i["id"])
        email_list=mysql_session.execute(get_email_query)
        email_list1=email_list.fetchone()[1]
    

        get_tables_query="""SELECT  group_id,group_concat("{2}","monitor_result/",csv_file_name,".csv") as email_list FROM {0}.fc_groups a
        inner join
        {0}.fc_group_table b
        on a.id=b.group_id
        inner join {0}.fc_monitor_tables c
        on b.table_id=c.id
        where group_id={1}
        group by group_id
        """.format(master_db_name,i["id"],MONITOR_SCRIPT_LOCATION)
        tables_list=mysql_session.execute(get_tables_query)
        table_list1=tables_list.fetchone()
        
        if table_list1 is not None:
            file_list = table_list1[1].split(',')
            title="""{0} DA: {1}""".format(CLIENT,monitor_title)
            body="""{}<br>{}<br><br>{}""".format(salutation,monitor_msg,regards)
            send_email(email_list1,title,body,file_list)
           

generate_monitor_result_csv = BashOperator( 
    task_id='generate_monitor_result_csv',
    bash_command='python '+MONITOR_SCRIPT_LOCATION+'generate_report.py generate_csv ',
    dag=dag,
    on_success_callback=notify_email_success,
    trigger_rule='all_done',
    params={
        "email_list":EMAIL_LIST,
        "task_success_msg":"csv dump of all monitor results have been successfully created in location: "+MONITOR_SCRIPT_LOCATION+"monitor_result/.",
        "task_failure_msg":"Failed to create csv dump of monitor results."
    }
)


send_mail = PythonOperator(
    task_id='send_mail',
    python_callable=group_wise_mail,
    dag=dag,
    #on_success_callback=notify_email_success,
    #on_failure_callback=notify_email_failure,
    params={
        "email_list":EMAIL_LIST,
        "task_success_msg":"Monitor results have been successfully sent to respective groups.",
        "task_failure_msg":"Failed to send monitor results to respective groups."
    },
    trigger_rule='all_done'
)

generate_monitor_result_csv>> send_mail


