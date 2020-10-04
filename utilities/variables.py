import os
file_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def initialize_json_config():
    import json

    global PYTHON
    global SCRIPT_LOCATION
    global AIRFLOW_HOME
    global EMAIL_LIST
    global START_DATE
    global CRON_EXPRESSION

    with open(os.path.abspath(file_path + '/config/config.json'), 'r') as js:
        js_conf = json.load(js)

    PYTHON = js_conf['PYTHON']
    SCRIPT_LOCATION = js_conf['SCRIPT_LOCATION']
    AIRFLOW_HOME = js_conf['AIRFLOW_HOME']
    EMAIL_LIST = js_conf['EMAIL_LIST']
    START_DATE = js_conf['START_DATE']
    CRON_EXPRESSION = js_conf['CRON_EXPRESSION']
    
initialize_json_config()
