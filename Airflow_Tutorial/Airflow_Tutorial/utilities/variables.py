import os
file_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

def initialize_json_config():
    import json
    global PYTHON
    global AIRFLOW_HOME
    global EMAIL_LIST
    global START_DATE  
    global CRON_EXPRESSION
    global PENTAHO_LOCATION
    global DEVELOP_SW_FILE_LOCATION
    global DEVELOP_RP_FILE_LOCATION
    global QA_SW_FILE_LOCATION
    global QA_RP_FILE_LOCATION
    global QA_VIEW_FILE_LOCATION
    global KPI_QA_LOCATION
    global host
    global username
    global password
    global master
    global client_master
    global sw_db
    global rp_db
    global sw_industry_db
    global qa_db
    global report_db
    global airflow_db
 

    with open(os.path.abspath(file_path + '/config/config.json'), 'r') as js:
        js_conf = json.load(js)

    PYTHON = js_conf['PYTHON']
    AIRFLOW_HOME = js_conf['AIRFLOW_HOME']
    EMAIL_LIST = js_conf['EMAIL_LIST']
    START_DATE = js_conf['START_DATE']
    CRON_EXPRESSION = js_conf['CRON_EXPRESSION']
    PENTAHO_LOCATION = js_conf['PENTAHO_LOCATION']
    DEVELOP_SW_FILE_LOCATION = js_conf['DEVELOP_SW_FILE_LOCATION']
    DEVELOP_RP_FILE_LOCATION = js_conf['DEVELOP_RP_FILE_LOCATION']
    QA_SW_FILE_LOCATION = js_conf['QA_SW_FILE_LOCATION']
    QA_RP_FILE_LOCATION = js_conf['QA_RP_FILE_LOCATION']
    QA_VIEW_FILE_LOCATION = js_conf['QA_VIEW_FILE_LOCATION']
    KPI_QA_LOCATION = js_conf['KPI_QA_LOCATION']
    host = js_conf['host']
    username = js_conf['username']
    password = js_conf['password']
    master = js_conf['master']
    client_master = js_conf['client_master']
    sw_db = js_conf['sw_db']
    rp_db = js_conf['rp_db']
    sw_industry_db =js_conf['sw_industry_db']
    qa_db = js_conf['qa_db']
    report_db = js_conf['report_db']
    airflow_db = js_conf['airflow_db']

initialize_json_config()

