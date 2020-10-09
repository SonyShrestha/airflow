# Description: Global declaration of variables
# Date: 25th September, 2020


import os
from sqlalchemy import create_engine
from utilities.db_con import *


file_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def initialize_json_config():
    import json

    global PYTHON_LOCATION
    global AIRFLOW_HOME

    global SQL_SCRIPT_LOCATION
    global MONITOR_SCRIPT_LOCATION
    global DA_SCRIPT_LOCATION
    global MIGRATE_DB_SCRIPT_LOCATION

    global EMAIL_LIST

    global CRON_EXPRESSION_DA
    global CRON_EXPRESSION_DA_Production_Point
    global CRON_EXPRESSION_Mail_Monitor_Result

    global START_DATE
    global CLIENT

    global mysql_url

    global mysql_config
    global mysql_driver
    global mysql_host
    global mysql_username
    global mysql_password

    global oracle_url
    global oracle_config
    global oracle_host
    global oracle_username
    global oracle_driver
    global oracle_password
    global oracle_port
    global oracle_sid

    global master_db_name
    global processing_db_name
    global app_db_name
    global raw_db_name
    
  
    with open(os.path.abspath(file_path + '/config/config.json'), 'r') as js:
        js_conf = json.load(js)

    CLIENT=js_conf['CLIENT']
    PYTHON_LOCATION = js_conf['PYTHON_LOCATION']
    AIRFLOW_HOME = js_conf['AIRFLOW_HOME']

    SQL_SCRIPT_LOCATION = js_conf['SQL_SCRIPT_LOCATION']
    MONITOR_SCRIPT_LOCATION = js_conf['MONITOR_SCRIPT_LOCATION']
    DA_SCRIPT_LOCATION = js_conf['DA_SCRIPT_LOCATION']
    MIGRATE_DB_SCRIPT_LOCATION = js_conf['MIGRATE_DB_SCRIPT_LOCATION']

    EMAIL_LIST=js_conf['EMAIL_LIST']

    CRON_EXPRESSION_DA = js_conf['CRON_EXPRESSION_DA']
    CRON_EXPRESSION_DA_Production_Point = js_conf['CRON_EXPRESSION_DA_Production_Point']
    CRON_EXPRESSION_Mail_Monitor_Result = js_conf['CRON_EXPRESSION_Mail_Monitor_Result']

    START_DATE = js_conf['START_DATE']



    master_db_name=js_conf['master_db_name']
    processing_db_name=js_conf['processing_db_name']
    app_db_name=js_conf['app_db_name']
    raw_db_name=js_conf['raw_db_name']





    mysql_config=js_conf['mysql_config']
    mysql_host=js_conf['mysql_config']['host']
    mysql_driver=js_conf['mysql_config']['driver']
    mysql_username=js_conf['mysql_config']['username']
    mysql_password=js_conf['mysql_config']['password']
    
    oracle_config=js_conf['oracle_config']
    oracle_host=js_conf['oracle_config']['host']
    oracle_driver=js_conf['oracle_config']['driver']
    oracle_username=js_conf['oracle_config']['username']
    oracle_password=js_conf['oracle_config']['password']
    oracle_sid=js_conf['oracle_config']['sid_name']
    oracle_port=js_conf['oracle_config']['port']

    mysql_url=get_con_url(**js_conf["mysql_config"])
    oracle_url=get_oracle_con_url(**js_conf["oracle_config"])

   

def get_con_url(**conf):
    return conf["driver"]+"://"+conf["username"]+":"+conf["password"]+"@"+conf["host"]
  

def get_oracle_con_url(**client_con):
    client_con_url = ("{driver}://{username}:{password}@(DESCRIPTION = "
                      "(LOAD_BALANCE=on) (FAILOVER=ON) "
                      "(ADDRESS = (PROTOCOL = TCP)(HOST = {host})(PORT = {port})) "
                      "(CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = {sid})))")

    return client_con_url.format(username=client_con['username'],
                                 password=client_con['password'],
                                 host=client_con['host'],
                                 sid=client_con['sid_name'],
                                 port=client_con['port'],
                                 driver=client_con['driver']) 

initialize_json_config()


