# importing default modules
import os
import shutil

# importing airflow modules
from airflow.utils.email import send_email


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
    email_list = (contextDict['params']['email_list'])

    # email title.
    title = "Airflow alert: {} Passed".format(contextDict['task'].task_id)

    # email contents
    body = """
	Hi Everyone, <br>
	<br>
	The job {0} job ran successfully. Try number : {1}
	<br>
	<br>
	log: {2}
	<br><br>
	{3}
	<br>
	Regards,<br>
	Airflow Automated Mail	
	""".format(contextDict['task'].task_id, contextDict['ti'].try_number, get_log_location(contextDict),contextDict['params']['success_message'])
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
    title = "Airflow alert: {} Failed".format(contextDict['task'].task_id)

    # email contents
    body = """
	Hi Everyone, <br>
	<br>
	There's been an error in the {0} job. Try number : {1}
	<br>
	<br>
	log: {2}
	<br>
        <br>
        {3}
        <br>
	Regards,<br>
	Airflow Automated Mail	
	""".format(contextDict['task'].task_id, contextDict['ti'].try_number, get_log_location(contextDict),contextDict['params']['failure_message'])
    files = [f for f in files if os.path.exists(f)]
    if len(files) == 0:
        send_email(email_list, title, body)
    else:
        send_email(email_list, title, body, files)
