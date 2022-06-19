import io
import logging
from typing import Any

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule


POKEMON_API = 'https://pokeapi.co/api/v2/'
PERSONAL_DIRECTORY = 'Imankulov/'
LOG_FILE_NAME = 'pokemons_log.log'

handler_buffer_string = io.StringIO()
handler = logging.StreamHandler(handler_buffer_string)
handler_format = logging.Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s', datefmt='%d-%m-%Y %H:%M:%S')
handler.setFormatter(handler_format)
logger = logging.getLogger('pokemons_log')
logger.setLevel(logging.INFO)
logger.addHandler(handler)


def log_write(message: str, level: str) -> None:
    """
    Processes the message with the specified level and writes the last line 
    from the buffer to the log file on s3.
    """
    if level == "critical":
            logger.critical(message)
    elif level == "error":
            logger.error(message)
    elif level == "warning":
            logger.warning(message)
    elif level == "info":
            logger.info(message)
    elif level == "debug":
            logger.debug(message)
    last_log_record = handler_buffer_string.getvalue().split('\n')[-2]

    s3_hook = S3Hook()
    snowpipe_files = Variable.get('snowpipe_files')
    bucket_name = snowpipe_files.split('/')[2]
    prefix = snowpipe_files.split('/')[3]
    files_paths = s3_hook.list_keys(bucket_name=bucket_name, prefix=f'{prefix}/{PERSONAL_DIRECTORY}')

    log_file = [file_path for file_path in files_paths if LOG_FILE_NAME in file_path][0]
    log_file = log_file.replace(f'{prefix}/', snowpipe_files)
    log_file_content = s3_hook.read_key(log_file)
    log_file_content = f'{log_file_content}\n{last_log_record}'

    target_file = f'{snowpipe_files}{PERSONAL_DIRECTORY}{LOG_FILE_NAME}'
    s3_hook.load_string(string_data=log_file_content, key=target_file, replace=True)


def print_message(message: str) -> None:
    """
    Displays a message in the arflow terminal.
    """
    print(message)


def get_count_generation() -> None:
    """
    Retrieves the count of available resources for this endpoint ('generation').
    """
    url = f'{POKEMON_API}generation/'
    count = requests.get(url).json()['count']
    log_write(f'Current number of generations is {count}', 'info')


with DAG(
    dag_id='Imankulov_FP_2',
    schedule_interval='@daily',
    start_date=days_ago(0),
    catchup=False,
    max_active_runs=1,
    tags=['de_school', 'check count generation']
    ) as dag:

    start = PythonOperator(
        task_id='start_task',
        python_callable=print_message,
        op_args=['Start']
        )

    count_generation = PythonOperator(
        task_id='count_generation_task',
        python_callable=get_count_generation
        )

    success = PythonOperator(
        task_id='success_task',
        python_callable=print_message,
        op_args=['DAG finished successfully']
        )

    failed = PythonOperator(
        task_id='failed_task',
        python_callable=print_message,
        op_args=['DAG finished failure'],
        trigger_rule=TriggerRule.ONE_FAILED
        )

    start >> count_generation >> [success, failed]
  
