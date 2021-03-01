import copy
import yaml
import traceback
from glob import glob
import pathlib
from airflow.models import DAG, Variable
from datetime import datetime, timedelta
from airflow import macros
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from common.file_utils import to_str_ds, to_str
import common.snowflake_utils as su
import common.yaml_includer as yml_includer
from airflow.operators import BashOperator
import subprocess
import requests
import json
import os
import pathlib

dags_dir = os.path.dirname(os.path.abspath(__file__))
print(dags_dir)
file_path = "/".join(dags_dir.split("/")[:3])
AIRFLOW_REPO_NAME = dags_dir.split("/")[2]
print("Airflow repo: ==>", AIRFLOW_REPO_NAME)
print("file_path is:==>", file_path)
ENV = Variable.get("ENV")
ENV_LOWER = ENV.lower()
FRAMEWORK_PATH = "/deploy/airflow/max-datascience-content-framework"
POSTFIX_DASH_ENV = '' if ENV == 'PROD' else '-dev'
POSTFIX_UNDERSCORE_ENV = '' if ENV == 'PROD' else '_dev'

SNOWFLAKE_ACCOUNT_NAME = Variable.get('SNOWFLAKE_ACCOUNT_NAME')
DAG_GENERATOR_EMR_YAML_CONFIG_PATH = dags_dir + "/resources/config/"
print("yml path====>", DAG_GENERATOR_EMR_YAML_CONFIG_PATH)
DAG_NAME_SEPARATOR = "_"
PIPELINE_LOGGER_SCRIPT = '/dags/resources/logging/max_event_processing_execution_log.sql'

webhook = Variable.get("slack_url")
DEFAULT_PRIORITY_WEIGHT = 50000
DEFAULT_TIMEOUT = 6*60*60
DEFAULT_DEPENDS_ON_PAST = False
DEFAULT_TEMPLATES_DICT = {'loop_index': 0}
DEFAULT_TASK_LOOP_SEED = 0
DEFAULT_TASK_LOOP_STEP = 1
DEFAULT_SQL_LOOP_SEED = 0
DEFAULT_SQL_LOOP_STEP = 1
yr = '{{macros.yr(ds)}}'
mo = '{{macros.mo(ds)}}'
ds_date = '{{ds}}'
dt = '{{macros.ds_format(ds, "%Y-%m-%d",  "%d")}}'

dag_dict = {}
config_loc = DAG_GENERATOR_EMR_YAML_CONFIG_PATH
file_name_format = "*.yml"


def slack_attachment(status, context):
    priority = {'DEV': 'HIGH', 'BETA': 'HIGH', 'PROD': 'CRITICAL'}
    hex_clr = {'failed': '#ff0400', 'retry': '#fbff00', 'success': '#04FC08'}
    attachments = [
        {
            "fallback": "Test.",
            "color": "{}".format(hex_clr[status]),
            "title": "Environment : {}".format(ENV.upper()),
            "fields": [
                {
                        "title": "Priority",
                        "value": "{}".format(priority[ENV.upper()]),
                        "short": False
                }
            ],
            # "text": context,
            "text": "The DAG {} is in *{}* state and the details below:\n*TASK:* {}\n*EXCEPTION OCCURRED:* {}\n Please take action.".format(context['dag'].dag_id, status, context['task'].task_id, context['exception']) if status in ['failed', 'retry'] else "The below task has *succeeded* for today's run \n*TASK:* {}".format(context['task'].task_id),
            "footer": "airflow-alert"
        }
    ]
    return attachments


def create_dag(dag_config):
    """
    Create the DAG dynamically based on the configration from yaml
    :param dag_config:
    :return: dag
    """
    # Try to load from the Variable incase the given is not in the date format
    try:
        start_date = datetime.strptime(
            dag_config['default_args']['start_date'], '%Y-%m-%d')
    except ValueError:
        start_date = datetime.strptime(Variable.get(
            dag_config['default_args']['start_date']), '%Y-%m-%d')
        if 'start_date_delta_days' in dag_config['default_args']:
            start_date = start_date + \
                timedelta(days=dag_config['default_args']
                          ['start_date_delta_days'])

    if 'end_date' in dag_config['default_args']:
        try:
            end_date = datetime.strptime(
                dag_config['default_args']['end_date'], '%Y-%m-%d')
        except ValueError:
            end_date = datetime.strptime(Variable.get(
                dag_config['default_args']['end_date']), '%Y-%m-%d')
    else:
        end_date = None

    SLACK_ENV = {'DEV': 'NON-PROD', 'BETA': 'NON-PROD', 'PROD': 'PROD'}
    channel = dag_config.get('slack_channel', {}).get(SLACK_ENV[ENV], None)

    def slack_success(context):
        response = requests.post(webhook,  data=json.dumps({"channel": channel, "username": "Sagemaker Job Alerts", "text": "*DAG ALERT:{} - SUCCESS*".format(
            context['dag'].dag_id), "attachments": slack_attachment('success', context)})) if ((channel is not None) and (len(channel) > 0)) else None
        if (response is not None) and response.status_code != 200:
            print("Unable to invoke slack api status code= ",
                  response.status_code, " Message = ", response.text)

    def slack_failure(context):
        response = requests.post(webhook,  data=json.dumps({"channel": channel, "username": "Sagemaker Job Alerts", "text": "*DAG ALERT:{} - FAILED*".format(
            context['dag'].dag_id), "attachments": slack_attachment('failed', context)})) if ((channel is not None) and (len(channel) > 0)) else None
        if (response is not None) and response.status_code != 200:
            print("Unable to invoke slack api status code= ",
                  response.status_code, " Message = ", response.text)

    def slack_retry(context):
        response = requests.post(webhook,  data=json.dumps({"channel": channel, "username": "Sagemaker Job Alerts", "text": "*DAG ALERT:{} - RETRY*".format(
            context['dag'].dag_id), "attachments": slack_attachment('retry', context)})) if ((channel is not None) and (len(channel) > 0)) else None
        if (response is not None) and response.status_code != 200:
            print("Unable to invoke slack api status code= ",
                  response.status_code, " Message = ", response.text)

    default_args = {
        'owner': dag_config['default_args']['owner'],
        'depends_on_past': dag_config['default_args']['depends_on_past'],
        'start_date': start_date,
        'end_date': end_date,
        'email': [dag_config['default_args']['email_prod']] if ENV == 'PROD' else [dag_config['default_args']['email_nonprod']],
        'email_on_failure': dag_config['default_args']['email_on_failure'],
        'email_on_retry': dag_config['default_args']['email_on_retry'],
        'on_failure_callback': slack_failure,
        # 'on_success_callback': slack_success,
        'on_retry_callback': slack_retry,
        'retries': dag_config['default_args']['retries'],
        'domain': dag_config['default_args']['domain'],
        'retry_delay': timedelta(minutes=dag_config['default_args']['retry_delay'])
    }
    dag = DAG(dag_config['dag_id'],
              schedule_interval=dag_config.get('schedule_interval') if (
                  "None" != dag_config.get('schedule_interval')) else None,
              catchup=dag_config.get('catchup') if (
                  "None" != dag_config.get('catchup')) else False,
              default_args=default_args,
              max_active_runs=dag_config.get('max_active_runs') if ("None" != dag_config.get(
                  'max_active_runs')) else configuration.conf.getint('core', 'max_active_runs_per_dag')
              )
    with dag:
        dag_dict[dag.dag_id] = {}
        last_task = None
        for task_loop_index, task_details in enumerate(dag_config['tasks'], 0):
            last_task = create_task(dag, dag_config['tasks'][task_loop_index], dag_config['tasks'], dag_config, str(
                dag_config['default_args']['owner']), dag_config['schema'], dag_config['snowflake_config'], dag_config['dynamo_cred_tbl'], dag_config.get('model_name', dag_config.get('glue_model_name')))
            dag_dict[dag.dag_id][dag_config['tasks']
                                 [task_loop_index]['name']] = last_task
        last_task.on_success_callback = slack_success
        pipeline_tasks(dag, dag_config['tasks'])
    return dag


def generate_looped_template_dict_sql(task_details, pipeline_dir, templates_dict):
    """
    :param task_details:
    :return:
    """

    sql_loop = task_details['sql_loop']
    sql_loop_seed = task_details['sql_loop_seed'] if 'sql_loop_seed' in task_details else DEFAULT_SQL_LOOP_SEED
    sql_loop_step = task_details['sql_loop_step'] if 'sql_loop_step' in task_details else DEFAULT_SQL_LOOP_STEP
    sql_loop_end = sql_loop_seed + sql_loop * sql_loop_step

    templates_dict_new = {'sql_loop': sql_loop}
    sql_file_name = task_details['sql_file_names'][0]
    sql_commands_raw = to_str_ds(str(file_path)+pipeline_dir + sql_file_name)
    sql_commands = ""

    for i, inc_value in enumerate(range(sql_loop_seed, sql_loop_end, sql_loop_step)):
        sql_commands = sql_commands + "\n" + sql_commands_raw
        for key, value in templates_dict.items():
            new_key = key + '_' + str(i)
            templates_dict_new[new_key] = str(value).replace(
                "sql_loop_index", str(inc_value))
            old_sql_key = "{" + str(key) + "}"
            new_sql_key = "{" + str(new_key) + "}"
            sql_commands = sql_commands.replace(old_sql_key, new_sql_key)

    templates_dict_new['sql_commands'] = sql_commands
    templates_dict_new['sql_loop'] = sql_loop

    return templates_dict_new


def dag_dict_lookup(keys, default=None):
    """
    Lookup function on dag dict to get the task for a dag
    :param keys:
    :param default:
    :return:
    """
    _current = dag_dict
    for key in keys:
        if key in _current:
            _current = _current[key]
        else:
            return default
    return _current


def get_task(dag, task_id):
    """
    Fetch the tasks instance based on task_id for a DAG
    :param dag:
    :param task_id:
    :return:
    """
    task = dag_dict_lookup([dag.dag_id, task_id])
    if task is None:
        print("Error: Task Not Found dag_id= ",
              dag.dag_id, " and task_id= ", task_id)
    return task


def create_task(dag, task_details, all_tasks, dag_config, owner, sql_schema_dag_level, snowflake_config, dynamo_cred_tbl, model):
    #print('Task Details dict = {}'.format(task_details))
    if 'type' in task_details and task_details['type'] == 'BashOperator':
        #print("Here============>", dag_config)
        return create_bash_operator_task(dag, task_details, all_tasks, dag_config, owner)

    elif 'type' in task_details and task_details['type'] == 'TriggerDagRunOperator':
        return create_trigger_task(dag, task_details)

    elif 'type' in task_details and task_details['type'] == 'ExternalTaskSensor':
        return create_external_task_sensor(dag, task_details)

    elif 'type' in task_details and task_details['type'] == 'EmailOperator':
        return create_email_task(dag, task_details)

    else:
        if task_details.get('model_type') == 'glue':
            model = dag_config.get('glue_model_name')
            pipeline_dir = "/Glue_Models/"+model+"/pipeline/"
        else:
            pipeline_dir = "/Models/"+model+"/pipeline/"
        # print(pipeline_dir)

        def _python_func(**kwargs):

            if 'kwargs' in task_details:
                kwargs.update(task_details['kwargs'])

            if 'templates_dict' in task_details or 'airflow_var_dict' in task_details:
                kwargs.update(kwargs['templates_dict'])

            sql_schema = task_details.get('sql_schema') if (
                "None" != task_details.get('sql_schema')) else sql_schema_dag_level

            if dynamo_cred_tbl is None:
                conn = su.connect(SNOWFLAKE_ACCOUNT_NAME, snowflake_config['snowflake_db_name'], sql_schema, snowflake_config['snowflake_warehouse_name'], snowflake_config['snowflake_etl_role'], snowflake_config['snowflake_environment_name']
                                  )
            else:
                conn = su.connect(SNOWFLAKE_ACCOUNT_NAME, snowflake_config['snowflake_db_name'], sql_schema, snowflake_config['snowflake_warehouse_name'], snowflake_config['snowflake_etl_role'], snowflake_config['snowflake_environment_name'], dynamo_cred_tbl
                                  )
            pipeline_sequence = []
            if 'sql_loop' in task_details:

                sql_commands = kwargs['sql_commands'] + \
                    "\n" + to_str(PIPELINE_LOGGER_SCRIPT)
                sql_commands = sql_commands.format(
                    dag_id=dag.dag_id, task_id=task_details['name'], **kwargs)
                su.execute_dml_statements(conn, sql_commands)
            else:

                for sql_file_name in task_details['sql_file_names']:
                    pipeline_sequence += [[pipeline_dir +
                                           sql_file_name, pipeline_dir + sql_file_name]]

                pipeline_sequence += [[PIPELINE_LOGGER_SCRIPT,
                                       PIPELINE_LOGGER_SCRIPT]]
                # print(pipeline_sequence)

                su.execute_sql_pipeline_sequence("", pipeline_sequence, conn, dag_id=dag.dag_id,
                                                 task_id=task_details['name'], file_path=str(file_path), **kwargs)
            su.disconnect(conn)

        priority_weight = task_details['priority_weight'] if 'priority_weight' in task_details else DEFAULT_PRIORITY_WEIGHT
        depends_on_past = task_details['depends_on_past'] if 'depends_on_past' in task_details else DEFAULT_DEPENDS_ON_PAST

        if 'templates_dict' in task_details:
            templates_dict = task_details['templates_dict']
            if 'task_loop_index' in task_details:
                for key, value in templates_dict.items():
                    templates_dict[key] = value.replace(
                        "task_loop_index", str(task_details['task_loop_index']))
            if 'sql_loop' in task_details:
                if len(task_details['sql_file_names']) > 1:
                    raise Exception(
                        "Only one sql file is supported along with sql_loop")
                templates_dict = generate_looped_template_dict_sql(
                    task_details, pipeline_dir, templates_dict)

        else:
            templates_dict = DEFAULT_TEMPLATES_DICT

        if 'airflow_var_dict' in task_details:
            airflow_var_dict = task_details['airflow_var_dict']
            #print("airflow_var_dict before update=", airflow_var_dict)
            for key, value in airflow_var_dict.items():
                #print("airflow_var_dict key=", key, " value=",value)
                airflow_var_dict[key] = Variable.get(value)
            templates_dict.update(airflow_var_dict)

        sf_executor = PythonOperator(
            task_id=task_details['name'],
            provide_context=True,
            python_callable=_python_func,
            priority_weight=priority_weight,
            depends_on_past=depends_on_past,
            templates_dict=templates_dict,
            dag=dag)

        return sf_executor


def create_bash_operator_task(dag, task_details, all_tasks, dag_config, owner):
    """
       Estabilish in Airflow
    """
    if 'ecr_container_creation' in task_details['name']:
        req_file_name = task_details.get(
            'requirements_file_name', 'requirements') + '.txt'
        command = """cd {0}
        python3 container_creation.py --model "{1}" --env "{2}" --kernel "{3}" --requirements_file_name "{4}" --airflow_repo "{5}" """.format(
            str(FRAMEWORK_PATH), str(dag_config['model_name']), str(ENV_LOWER),  task_details['kernel'], req_file_name, AIRFLOW_REPO_NAME)

    if 'glue_job_processing_script' in task_details['name']:
        bucket = ""
        if ENV_LOWER == "dev":
            bucket = task_details['BucketIOConfig']['DEV']['model_bucket']
        elif ENV_LOWER == "beta":
            bucket = task_details['BucketIOConfig']['BETA']['model_bucket']
        else:
            bucket = task_details['BucketIOConfig']['PROD']['model_bucket']

        param_file_name = task_details['Details'].get(
            'param_file_name', 'parameters') + ".json"
        req_file_name = task_details['Details'].get(
            'requirements_file_name', 'requirements') + '.txt'

        dag_details = task_details.get('templates_dict', {})
        timeout = task_details['Details'].get('timeout', 180)
        #print("timeout", timeout)
        #print("dag_details are:",dag_details)
        command = """cd %s
        python3 glue_job_processor.py --model "%s" --script_name "%s"  --param_file "%s" --requirements_file_name "%s" --glue_version "%s" --worker_type "%s" --number_of_workers "%s" --job_type "%s" --env "%s" --owner_name "%s" --bucket "%s" --script_loc "%s" --dag_details "%s" --timeout "%s" --airflow_repo "%s" """ % ( str(FRAMEWORK_PATH), str(dag_config['glue_model_name']),
                                                                                                                                                                                                                                                                                                                                                                  str(
                                                                                                                                                                                                                                                                                                                                                                      task_details['Details']['script_name']),
                                                                                                                                                                                                                                                                                                                                                                  param_file_name,
                                                                                                                                                                                                                                                                                                                                                                  req_file_name,
                                                                                                                                                                                                                                                                                                                                                                  "2.0",
                                                                                                                                                                                                                                                                                                                                                                  str(
                                                                                                                                                                                                                                                                                                                                                                      task_details['Details']['worker_type']),
                                                                                                                                                                                                                                                                                                                                                                  str(
                                                                                                                                                                                                                                                                                                                                                                      task_details['Details']['number_of_workers']),
                                                                                                                                                                                                                                                                                                                                                                  "Spark",
                                                                                                                                                                                                                                                                                                                                                                  str(
                                                                                                                                                                                                                                                                                                                                                                      ENV_LOWER),
                                                                                                                                                                                                                                                                                                                                                                  str(
                                                                                                                                                                                                                                                                                                                                                                      owner),
                                                                                                                                                                                                                                                                                                                                                                  str(
                                                                                                                                                                                                                                                                                                                                                                      bucket),
                                                                                                                                                                                                                                                                                                                                                                  task_details['Details'][
                                                                                                                                                                                                                                                                                                                                                                      'script_loc'],
                                                                                                                                                                                                                                                                                                                                                                  dag_details,
                                                                                                                                                                                                                                                                                                                                                                  str(
                                                                                                                                                                                                                                                                                                                                                                      timeout),
                                                                                                                                                                                                                                                                                                                                                                  str(AIRFLOW_REPO_NAME)
                                                                                                                                                                                                                                                                                                                                                                  )

    if 'sagemaker_notebook_job_processing_script' in task_details['name']:

        upstream = task_details['upstream_task_name']
        for tsk in all_tasks:
            if tsk['name'] == upstream:
                kernel = tsk['kernel']

        bucket = ""
        if ENV_LOWER == "dev":
            bucket = task_details['BucketIOConfig']['DEV']['model_bucket']
        elif ENV_LOWER == "beta":
            bucket = task_details['BucketIOConfig']['BETA']['model_bucket']
        else:
            bucket = task_details['BucketIOConfig']['PROD']['model_bucket']
        param_file_name = task_details.get(
            'param_file_name', 'parameters') + ".json"
        dag_details = task_details.get('templates_dict', {})
        volume_size = task_details['Details'].get('volume_size_InGB', 50)
        command = """cd %s
        python3 sg_job_processor.py --instance_type "%s" --instance_count "%s" --bucket "%s" --model "%s" --notebook_name "%s" --owner_name "%s" --env "%s" --param_file "%s" --kernel "%s" --volume_size_InGB "%s" --dag_details "%s" --airflow_repo "%s" """ % (str(FRAMEWORK_PATH), str(task_details['Details']['instance_type']),
                                                                                                                                                                                                                                                                                                   task_details['Details'][
                                                                                                                                                                                                                                                                                                       'instance_count'],
                                                                                                                                                                                                                                                                                                   str(
                                                                                                                                                                                                                                                                                                       bucket),
                                                                                                                                                                                                                                                                                                   str(
                                                                                                                                                                                                                                                                                                       dag_config['model_name']),
                                                                                                                                                                                                                                                                                                   str(
                                                                                                                                                                                                                                                                                                       task_details['notebook_name']),
                                                                                                                                                                                                                                                                                                   str(
                                                                                                                                                                                                                                                                                                       owner),
                                                                                                                                                                                                                                                                                                   str(
                                                                                                                                                                                                                                                                                                       ENV_LOWER),
                                                                                                                                                                                                                                                                                                   param_file_name,
                                                                                                                                                                                                                                                                                                   kernel,
                                                                                                                                                                                                                                                                                                   str(
                                                                                                                                                                                                                                                                                                       volume_size),
                                                                                                                                                                                                                                                                                                   dag_details,
                                                                                                                                                                                                                                                                                                   str(
                                                                                                                                                                                                                                                                                                       AIRFLOW_REPO_NAME)
                                                                                                                                                                                                                                                                                                   )

    if task_details['name'].startswith('python_script'):
        if task_details['script_loc'] == "model" and task_details.get('model_type') == 'sagemaker':
            model_root = "Models"
            model = dag_config['model_name']
            base_folder = "/python_scripts"
        elif task_details['script_loc'] == "model" and task_details.get('model_type') == 'glue':
            model_root = "Glue_Models"
            model = dag_config['glue_model_name']
            base_folder = "/python_scripts"
        elif task_details['script_loc'] == "common":
            model_root = "Py_Models"
            model = task_details['script_name']
            base_folder = ""

        # params={}
        def_path = "{}/{}/{}{}".format(
                file_path, model_root, model, base_folder)
        param_file_loc = "{}/{}.json".format(def_path,
                                             task_details['param_file_name'])
        params_str = ""
        dag_details = task_details.get('templates_dict', {})
        if os.path.exists(param_file_loc):
            with open(param_file_loc) as f:
                data = json.load(f)
                params_temp = data.get(
                    ENV.upper(), data.get(ENV.lower(), data))
                common_param = data.get("COMMON", data.get("common", {}))
                params_temp.update(common_param)
                params_temp.update(dag_details)
                for i, j in params_temp.items():
                    temp = ""
                    temp += "--"+str(i)
                    params_str += " {} {}".format(temp, j)
        else:
            print("Mentioned param file doesn't exist so passing empty paramters.")

        command = "python3 {}/{}.py {}".format(
            def_path, task_details['script_name'], params_str)
        # print(command)

    bash_Operator_task = BashOperator(
        task_id=task_details['name'],
        bash_command=command,
        dag=dag
    )

    return bash_Operator_task


def generate_dag_id(dag_config):
    return "{domain}{sep}{sub_domain}{sep}{action}".format(
        domain=dag_config.get("domain"),
        sub_domain=dag_config.get("sub_domain"),
        action=dag_config.get("action"),
        sep=DAG_NAME_SEPARATOR)


def create_external_task_sensor(dag, task_details):
    """
    creates ExternalTaskSensor task
    :param dag:
    :param task_details:
    :return:
    """
    external_task = task_details['external_dag_details'].get(
        'external_task_id', None)
    poke_interval = task_details['external_dag_details'].get(
        'poke_interval', 60)
    mode = task_details['external_dag_details'].get('mode', 'poke')
    execution_delta = task_details['external_dag_details'].get(
        'execution_delta', 'timedelta(hours=0, minutes=0)')
    timeout = task_details['external_dag_details'].get('timeout', 60)

    external_dag_task_sensor = ExternalTaskSensor(
        task_id=task_details['name'],
        external_dag_id=task_details['external_dag_details']['external_dag_id'],
        external_task_id=external_task if str(
            external_task).lower() != 'none' else None,
        allowed_states=task_details['external_dag_details'].get(
            'allowed_states', 'success').split(","),
        poke_interval=eval(poke_interval) if isinstance(
            poke_interval, str) else poke_interval,
        mode=mode,
        timeout=eval(timeout) if isinstance(timeout, str) else timeout,
        execution_delta=eval(execution_delta),
        dag=dag
    )
    return external_dag_task_sensor


def create_trigger_task(dag, task_details):
    """
    creates TriggerDagRunOperator task
    :param dag:
    :param task_details:
    :return:
    """

    timeout = task_details['timeout'] if 'timeout' in task_details else DEFAULT_TIMEOUT
    depends_on_past = task_details['depends_on_past'] if 'depends_on_past' in task_details else DEFAULT_DEPENDS_ON_PAST

    trigger_task = TriggerDagRunOperator(
        task_id=task_details['name'],
        trigger_dag_id=task_details['trigger_dag'],
        dag=dag
    )
    return trigger_task


def create_email_task(dag, task_details):
    """
    creates TriggerDagRunOperator task
    :param dag:
    :param task_details:
    :return:
    """
    notify_task = EmailOperator(
        mime_charset='utf-8',
        task_id=task_details['name'],
        to=task_details['to'],
        subject=task_details['subject'],
        html_content=task_details['html'],
        dag=dag
    )
    return notify_task


def pipeline_tasks(dag, tasks):
    """
    Function to pipeline the task based on configuration
    Make sure you are not creating any cyclic dependencies or duplicate dependencies while configuring via yaml
    :param dag:
    :param tasks:
    :return:
    """
    for task_details in tasks:
        #print("current_task_name=" + task_details['name'])
        if 'upstream_task_name' in task_details:
            #print("upstream_task_name=" + task_details['upstream_task_name'])
            get_task(dag, task_details['upstream_task_name']) >> get_task(
                dag, task_details['name'])
        if 'downstream_task_name' in task_details:
            #print("downstream_task_name=" + task_details['downstream_task_name'])
            get_task(dag, task_details['name']) >> get_task(
                dag, task_details['downstream_task_name'])


def get_sf_config(config):
    """
    fetch the environment specific SF details
        (For backward compatibility, if db_details is missing, then use the values from root level)
    :param config:
    :return:
    """
    sf_config = {}
    if 'db_details' in config:
        sf_config = config.get('db_details', {}).get(ENV)
    else:
        sf_config = {
            'snowflake_db_name':  config['snowflake_db_name'], 'snowflake_environment_name':  config['snowflake_environment_name'], 'snowflake_etl_role':  config['snowflake_etl_role'], 'snowflake_warehouse_name':  config['snowflake_warehouse_name']
        }
    return sf_config


yaml.add_constructor(
    '!include', yml_includer.construct_include, yml_includer.Loader)
for config_file in glob(config_loc+file_name_format):
    #print("Current yaml config file being processed: " + config_file)
    # Loads the config yaml and build DAG for each dag config
    with open(config_file, 'r') as file_stream:
        try:
            config = yaml.load(file_stream, yml_includer.Loader)
            # print(config)
            if 'CommonConf' in config:
                config.update(config['CommonConf'])
                del config['CommonConf']
            #print('Dict generated from config file = {}'.format(config))
            dags = config['dags']
            snowflake_config = get_sf_config(config)
            # print(dags)
            for name, dag_config in dags.items():
                print("creating the dag ", generate_dag_id(dag_config))
                try:
                    dag_config['dag_id'] = generate_dag_id(dag_config)
                    dag_config['snowflake_config'] = snowflake_config
                    dag_config['dynamo_cred_tbl'] = dag_config.get(
                        'dynamo_cred_tbl') if 'dynamo_cred_tbl' in dag_config else None
                    globals()[dag_config['dag_id']] = create_dag(dag_config)
                except Exception as ex:
                    traceback.print_exc()
                    print("Error: Failed to create dag ", name)
        except Exception as ex:
            traceback.print_exc()
            print("Error: Failed to create dag ", name)
