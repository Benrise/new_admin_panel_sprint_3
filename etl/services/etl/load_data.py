from time import sleep
from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.extensions import AsIs

from typing import Generator, Set

from elasticsearch import Elasticsearch
import elasticsearch

from utils.backoff import backoff
from utils.context_manager import closing
from utils.sql_queries import (
    sql_extract_last_updated_table_query, 
    sql_extract_updated_film_work_records_query, 
    schema
)
from utils.coroutine import coroutine
from utils.logger import logger
from utils.models import (
    Movie, 
    TransformedMovie, 
    PersonRolesEnum, 
    filter_persons
)

from state.json_file import JsonFileStorage
from state.main import State

from config import (
    DSL,
    ES,
    ETL,
    BACKOFF
)

STATE_UPDATED_KEY = 'last_updated_record'


"""
Source approach:
https://habr.com/ru/articles/710106/
"""

BACKOFF_START_SLEEP_TIME = int(BACKOFF["start_sleep_time"])
BACKOFF_FACTOR = int(BACKOFF["factor"])
BACKOFF_BORDER_SLEEP_TIME = int(BACKOFF["border_sleep_time"])

ETL_SLEEP_TIME = int(ETL["sleep_time"])

@backoff(BACKOFF_START_SLEEP_TIME, BACKOFF_FACTOR, BACKOFF_BORDER_SLEEP_TIME)
def connect_to_pg():
    with closing(psycopg2.connect(**DSL, cursor_factory=DictCursor)) as pg_conn:
        pg_conn = psycopg2.connect(**DSL, cursor_factory=DictCursor)
        curs = pg_conn.cursor()
        return curs

@backoff(BACKOFF_START_SLEEP_TIME, BACKOFF_FACTOR, BACKOFF_BORDER_SLEEP_TIME)
def connect_to_es():
    es_conn = Elasticsearch(ES["hosts"])
    if not es_conn.indices.exists(index=ES["index_name"]):
        es_conn.indices.create(index=ES["index_name"], settings=ES["index_settings"], mappings=ES["index_mappings"])
    return es_conn

@coroutine
def extract_changed_movies(state: State, cursor, next_node: Generator) -> Generator[None, any, None]:
    while True:
        table_name, last_updated_at = (yield)
        pkeys = []
        
        logger.info(f'[{table_name}] Fetching data changed after: {last_updated_at}')
        logger.info(f'[{table_name}] Fetching data updated after: {last_updated_at}\n')
        
        try:
            last_updated_vars = {
                'table': AsIs(schema + '.' + table_name),
                'updated_at': last_updated_at,
                'limit': 100
            }

            cursor.execute(sql_extract_last_updated_table_query, last_updated_vars)
            while results := cursor.fetchmany(size=100):
                last_updated_at = results[-1]['updated_at']
                state.set_state(table_name, str(last_updated_at))
                pkeys.extend([record[0] for record in results]) 
                
            sql_extract_from_last_updated_table_vars = {
                'table': AsIs(schema + '.' + table_name),
                'pkeys': tuple(pkeys),
                'last_id': state.get_state(STATE_UPDATED_KEY) or '',
                'limit': 100
            }
            
            if pkeys:
                cursor.execute(sql_extract_updated_film_work_records_query, sql_extract_from_last_updated_table_vars)
                while results := cursor.fetchmany(size=100):
                    set_batch_state(
                        state,
                        table=table_name,
                        pkeys=pkeys,
                        last_updated_id=results[-1]['id']
                    )
                    logger.info(f'[{table_name}] Got additional records for {len(results)} movies')
                    next_node.send(results)
                set_batch_state(
                    state,
                    table=None,
                    pkeys=None,
                    last_updated_id=None
                )
            else:
                logger.info(f"[{table_name}] No pkeys found for table. Skipping SQL query\n")
                
        except psycopg2.OperationalError:
            cursor = connect_to_pg()
            
def set_batch_state(state: State, **kwargs) -> None:
        for key, value in kwargs.items():
            state.set_state(key=key, value=str(value))

def get_last_updated_at(state, table_name) -> str:
    last_updated_at = state.get_state(table_name)
    return last_updated_at or datetime.min

@coroutine
def transform_movies(state: State, next_node: Generator) -> Generator[None, list[dict], None]:
    while movie_dicts := (yield):
        batch = []
        set_batch_state(state, data=movie_dicts)
        for movie_dict in movie_dicts:
            source_movie = Movie(**movie_dict)
            directors = filter_persons(source_movie.persons, PersonRolesEnum.DIRECTOR)
            actors = filter_persons(source_movie.persons, PersonRolesEnum.ACTOR)
            writers = filter_persons(source_movie.persons, PersonRolesEnum.WRITER)
            transformed_movie = TransformedMovie(
                id=source_movie.id,
                imdb_rating=source_movie.rating,
                genres=source_movie.genres,
                title=source_movie.title,
                description=source_movie.description,
                directors=[director for director in directors],
                actors_names=[actor.full_name for actor in actors],
                writers_names=[writer.full_name for writer in writers], 
                actors=[actor for actor in actors],
                writers=[writer for writer in writers]
            )

            batch.append(transformed_movie)
        set_batch_state(state, data=None)
        next_node.send(batch)

@coroutine
def load_movies(es_conn: Elasticsearch) -> Generator[None, list[TransformedMovie], None]:
    while movies := (yield):
        actions = []
        for movie in movies:
            action = {
                "index": {
                    "_index": ES["index_name"],
                    "_id": movie.id
                }
            }
            actions.append(action)
            actions.append(movie.model_dump_json())
        
        try:
            es_conn.bulk(body=actions)
        except elasticsearch.exceptions.ConnectionError:
            es_conn = connect_to_es()

if __name__ == "__main__":
    es_conn = connect_to_es()
    curs = connect_to_pg()
    state = State(JsonFileStorage('./state/movies_state.json'))
    loader_coro = load_movies(es_conn)
    transformer_coro = transform_movies(state, next_node=loader_coro)
    extractor_coro = extract_changed_movies(state, curs, transformer_coro)
    logger.info('Starting ETL process for updates ...')
    while True:
        for table_name in ETL["extract_tables"]:
            logger.info(f'[{table_name}] Checking updated records ...')
            extractor_coro.send((table_name, get_last_updated_at(state, table_name)))

            sleep(ETL_SLEEP_TIME)