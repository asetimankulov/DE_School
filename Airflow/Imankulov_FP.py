import json
import io
import logging
import threading
import time
from queue import Queue
from typing import List, Any

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

#form a handler for storing and writing log information
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


def get_urls(url: str, **context) -> None:
    """
    Gets all links placed on the getting url and passes them to xcom.
    """
    count = requests.get(url).json()['count']
    response = requests.get(f'{url}?limit={count}')
    body = response.json()

    urls = [url['url'] for url in body['results']]
    feature_name = url.split('/')[-2]

    context['task_instance'].xcom_push(key=f'{feature_name}', value=urls)
    log_write(f'Got {count} urls from {url}', 'info')


def merge_lists_urls(**context) -> None:
    """
    Merges lists with urls into one.
    """
    pokemons_urls_task = context['task_instance'].xcom_pull(task_ids='pokemons_urls_task', key='pokemon')
    generations_urls_task = context['task_instance'].xcom_pull(task_ids='generations_urls_task', key='generation')
    all_urls = pokemons_urls_task + generations_urls_task
    context['task_instance'].xcom_push(key='all_urls', value=all_urls)


def load_string_to_s3(file_name: str, data: Any) -> None:
    """
    Uploads a file with the specified data on s3 to the personal directory.
    """
    s3_hook = S3Hook()
    snowpipe_files = Variable.get('snowpipe_files')

    target_file = f'{snowpipe_files}{PERSONAL_DIRECTORY}{file_name}.json'
    data = str(data)

    s3_hook.load_string(string_data=data, key=target_file, replace=True)


def get_id_from_url(url: str) -> str:
    """
    Extracts the identifier from the url.
    """
    id_from_url = url.split('/')[-2]
    return id_from_url


def get_names(url: str) -> None:
    """
    Gets all identifiers and names of the specified resource (feature).
    """
    count = requests.get(url).json()['count']
    response = requests.get(f'{url}?limit={count}')
    body = response.json()
    names = [{'id': get_id_from_url(url['url']), 'name': url['name']} for url in body['results']]

    feature_name = url.split('/')[-2]
    feature = {feature_name: names}

    load_string_to_s3(feature_name, feature)

    log_write(f'Got {count} names from {url}', 'info')


def get_target_features(url_content: dict, target_features: List[str]) -> dict:
    """
    Get a value-key pair for the specified keys.
    """
    features = {k: url_content[k] for k in url_content if k in target_features}
    return features


def url_content_filter(url, content) -> dict:
    """
    Filters the resulting dictionary based on the name of the endpoint.
    Identifiers are retrieved from the content of the resource (feature), formed on the basis of the url.
    For the 'stat' attribute, the 'name' and 'base_stat' are also extracted.
    """
    if f'{POKEMON_API}pokemon/' in url:
        pokemon = get_target_features(content,
                                          ['id', 'name', 'moves', 'species', 'stats', 'types', 'past_types'])
        
        pokemon['moves'] = [{'id': get_id_from_url(move['move']['url'])}
                            for move in pokemon['moves']]
        pokemon['species'] = {'id': get_id_from_url(pokemon['species']['url'])}
        pokemon['stats'] = [{'base_stat': stat['base_stat'], 'name': stat['stat']['name'], 'id': get_id_from_url(stat['stat']['url'])} 
                            for stat in pokemon['stats']]
        pokemon['types'] = [{'id': get_id_from_url(pokemon_type['type']['url'])} 
                            for pokemon_type in pokemon['types']]
        pokemon['past_types'] = [{'generation': {'id': get_id_from_url(generation_types['generation']['url'])},
                                  'types': [{'id': get_id_from_url(past_type['type']['url'])}
                                            for past_type in generation_types['types']]}
                                for generation_types in pokemon['past_types']]
        return pokemon

    elif f'{POKEMON_API}generation/' in url:
        generation = get_target_features(content,
                                         ['id', 'name', 'pokemon_species'])
        generation['pokemon_species'] = [{'id': get_id_from_url(species['url']), 'name': species['name']}
                                        for species in generation['pokemon_species']]
        return generation


def get_url_content(urls_queue: Queue, request_per_sec: int, lock: threading.Lock) -> None:
    """
    Gets the content of each url from the queue and writes it to a file on s3 based on the name of the endpoint.
    A lock is used to avoid a race condition when writing to the same file.
    """
    s3_hook = S3Hook()
    snowpipe_files = Variable.get('snowpipe_files')

    while True:
        url = urls_queue.get()
        response = requests.get(url)
        body = response.json()
        body = url_content_filter(url, body)
        if f'{POKEMON_API}pokemon/' in url:
            file_name = 'pokemon.json'
        elif f'{POKEMON_API}generation/' in url:
            file_name = 'generation.json'
        
        lock.acquire()

        file_content = s3_hook.read_key(f'{snowpipe_files}{PERSONAL_DIRECTORY}{file_name}')
        if file_content == '':
            file_content = dict()
        else:
            file_content = file_content.replace("\'", "\"").replace('None', 'NaN')
            file_content = json.loads(file_content)

        file_content[str(body['id'])] = body

        load_string_to_s3(file_name.split('.')[0], file_content)

        lock.release()

        urls_queue.task_done()
        time.sleep(1 / request_per_sec)


def get_url_content_thread(thread_cnt: int = 2, request_per_sec: int = 5, **context) -> None:
    """
    Receives a list of urls from xcom and extracts their contents using multiple threads.
    Threads process a queue with a limit on the number of requests per second.
    """
    all_urls = context['task_instance'].xcom_pull(task_ids='all_urls_task', key='all_urls')

    all_urls_queue: Queue = Queue()
    for url in all_urls:
        all_urls_queue.put(url)

    lock = threading.Lock()

    threads = list()
    for _ in range(thread_cnt):
        thread = threading.Thread(target=get_url_content, args=(all_urls_queue, request_per_sec, lock,))
        threads.append(thread)
        thread.start()

    log_write(f'Created {thread_cnt} threads to receive data', 'info')

    log_write(f'Start getting content for {all_urls_queue.qsize()} urls', 'info')
    all_urls_queue.join()
    log_write(f'Finish getting content. The current queue size is {all_urls_queue.qsize()} urls', 'info')


with DAG(
    dag_id='Imankulov_FP',
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False,
    max_active_runs=1,
    tags=['de_school', 'fetch data']
    ) as dag:

    start = PythonOperator(
        task_id='start_task',
        python_callable=print_message,
        op_args=['Start']
        )

    pokemons_urls = PythonOperator(
        task_id='pokemons_urls_task',
        python_callable=get_urls,
        op_args=[f'{POKEMON_API}pokemon/']
        )

    generations_urls = PythonOperator(
        task_id='generations_urls_task',
        python_callable=get_urls,
        op_args=[f'{POKEMON_API}generation/']
        )

    all_urls = PythonOperator(
        task_id='all_urls_task',
        python_callable=merge_lists_urls
        )

    types = PythonOperator(
        task_id='types_task',
        python_callable=get_names,
        op_args=[f'{POKEMON_API}type/']
        )

    moves = PythonOperator(
        task_id='moves_task',
        python_callable=get_names,
        op_args=[f'{POKEMON_API}move/']
        )

    url_content = PythonOperator(
        task_id='url_content_task',
        python_callable=get_url_content_thread,
        op_args=[2, 5]
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

    start >> [pokemons_urls, generations_urls] >> all_urls >> [types, moves]
    [types, moves] >> url_content >> [success, failed]
  
