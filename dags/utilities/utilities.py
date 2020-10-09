# Description: Custom defined mail function for sending mail on success/ failure of task

# importing default modules
import os
import shutil


# importing airflow modules
from airflow.utils.email import send_email

from html_template.email_template import *
from utilities.variables import *

def get_log_location(contextDict):
    '''
	Getting the log location after archiving the logs.
	
 	Params:
		contextDict	: context dictionery from airflow
    '''
    log_folder = contextDict['ti'].log_filepath.split('.')[0]
    if os.path.exists(log_folder):
        shutil.make_archive(log_folder, 'zip', log_folder)
        return log_folder+'.zip'
    else:
        return contextDict['ti'].log_filepath


def notify_email_success(contextDict, **kwargs):
    """
    Send custom email alerts.
    
    Params:
		contextDict	: context dictionery from airflow
		**kwargs	: kwargs for extra params.
  
    """
    if 'file' in (contextDict['params']):
        files = (contextDict['params']['file'])
    else:
        files = []
    files.append(get_log_location(contextDict)) 
    print(files)
    email_list = (contextDict['params']['email_list'])

    # email title.
    title = "{} DA: {} {}".format(CLIENT,contextDict['task'].task_id,success_title)

    # email contents
    body = "{}<br>{} <br><br> {} <br><br> {}".format(salutation,success_body,contextDict['params']['task_success_msg'],regards)

    files = [f for f in files if os.path.exists(f)]
    if len(files) == 0:
        send_email(email_list, title, body)
    else:
        send_email(email_list, title, body, files)


def notify_email_failure(contextDict, **kwargs):
    """
    Send custom email alerts.
    
    Params:
		contextDict	: context dictionery from airflow
		**kwargs	: kwargs for extra params.
  
    """
    if 'file' in (contextDict['params']):
        files = (contextDict['params']['file'])
    else:
        files = []
    files.append(get_log_location(contextDict))
    email_list = (contextDict['params']['email_list'])

    # email title.
    title = "{} DA: {} {}".format(CLIENT,contextDict['task'].task_id,failure_title)

    # email contents
    body="{}<br>{}<br><br>{}<br><br>{}".format(salutation,failure_body,contextDict['params']['task_failure_msg'],regards)

    files = [f for f in files if os.path.exists(f)]
    if len(files) == 0:
        send_email(email_list, title, body)
    else:
        send_email(email_list, title, body, files)


