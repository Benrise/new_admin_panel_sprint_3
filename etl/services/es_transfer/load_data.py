from time import sleep
import psycopg2
from psycopg2.extras import DictCursor

from datetime import datetime

from typing import Generator

from utils.context_manager import closing
from utils.sql_queries import sql_extract_query
from utils.coroutine import coroutine
from utils.logger import logger

from state.json_file import JsonFileStorage
from state.main import State
from utils.models import Movie

from config import (
    DSL
)

"""
Source solution:
https://habr.com/ru/articles/710106/
"""

STATE_KEY = 'last_movies_updated'

@coroutine
def fetch_changed_movies(cursor, next_node: Generator) -> Generator[None, datetime, None]:
    while last_updated := (yield):
        logger.info(f'Fetching movies changed after %s', last_updated)
        logger.info('Fetching movies updated after %s', last_updated)
        vars = {
            'updated_at': last_updated,
            'limit': 100
        }
        cursor.execute(sql_extract_query, vars)
        while results := cursor.fetchmany(size=100):
            next_node.send(results)


@coroutine
def transform_movies(next_node: Generator) -> Generator[None, list[dict], None]:
    while movie_dicts := (yield):
        batch = []
        for movie_dict in movie_dicts:
            movie = Movie(**movie_dict)
            movie.title = movie.title.upper()
            logger.info(movie.json())
            batch.append(movie)
        next_node.send(batch)


@coroutine
def save_movies(state: State) -> Generator[None, list[Movie], None]:
    while movies := (yield):
        logger.info('Received for saving %s movies', len(movies))
        logger.info([movie.json() for movie in movies])
        state.set_state(STATE_KEY, str(movies[-1].updated_at))

if __name__ == "__main__":
    state = State(JsonFileStorage('./state/movies_state.json'))
    with closing(psycopg2.connect(**DSL, cursor_factory=DictCursor)) as pg_conn:
        curs = pg_conn.cursor()
        saver_coro = save_movies(state)
        transformer_coro = transform_movies(next_node=saver_coro)
        fetcher_coro = fetch_changed_movies(curs, transformer_coro)

        while True:
            last_movies_updated = state.get_state(STATE_KEY)
            logger.info('Starting ETL process for updates ...')

            fetcher_coro.send(state.get_state(STATE_KEY) or str(datetime.min))

            sleep(15)