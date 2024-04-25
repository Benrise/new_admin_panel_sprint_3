from time import sleep
import psycopg2
from psycopg2.extras import DictCursor

from datetime import datetime

from typing import Generator

from elasticsearch import Elasticsearch
import elasticsearch

from utils.backoff import backoff
from utils.context_manager import closing
from utils.sql_queries import sql_extract_query
from utils.coroutine import coroutine
from utils.logger import logger

from state.json_file import JsonFileStorage
from state.main import State
from utils.models import Movie, TransformedMovie, PersonRolesEnum, filter_persons

from config import (
    DSL,
    ES,
    ETL,
    BACKOFF
)

STATE_KEY = 'last_movie_updated'


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
    pg_conn = psycopg2.connect(**DSL, cursor_factory=DictCursor)
    curs = pg_conn.cursor()
    return pg_conn, curs

@backoff(BACKOFF_START_SLEEP_TIME, BACKOFF_FACTOR, BACKOFF_BORDER_SLEEP_TIME)
def connect_to_es():
    es_conn = Elasticsearch(ES["hosts"])
    if not es_conn.indices.exists(index=ES["index_name"]):
        es_conn.indices.create(index=ES["index_name"], settings=ES["index_settings"], mappings=ES["index_mappings"])
    return es_conn

@coroutine
def extract_changed_movies(cursor, next_node: Generator) -> Generator[None, datetime, None]:
    while last_updated := (yield):
        logger.info(f'Fetching movies changed after %s', last_updated)
        logger.info('Fetching movies updated after %s', last_updated)
        vars = {
            'updated_at': last_updated,
            'limit': 100
        }
        try: 
            cursor.execute(sql_extract_query, vars)
            while results := cursor.fetchmany(size=100):
                next_node.send(results)
        except psycopg2.OperationalError:
            connect_to_pg()

@coroutine
def transform_movies(state: State, next_node: Generator) -> Generator[None, list[dict], None]:
    while movie_dicts := (yield):
        batch = []
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
            state.set_state(STATE_KEY, str(movie_dict["updated_at"]))
            batch.append(transformed_movie)
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
            connect_to_es()

if __name__ == "__main__":
    es_conn = connect_to_es()
    pg_conn, curs = connect_to_pg()
    state = State(JsonFileStorage('./state/movies_state.json'))
    loader_coro = load_movies(es_conn)
    transformer_coro = transform_movies(state, next_node=loader_coro)
    extractor_coro = extract_changed_movies(curs, transformer_coro)

    while True:
        last_movies_updated = state.get_state(STATE_KEY)
        logger.info('Starting ETL process for updates ...')

        extractor_coro.send(state.get_state(STATE_KEY) or str(datetime.min))

        sleep(ETL_SLEEP_TIME)