from contextlib import contextmanager
import logging
import time

from process.producer import Producer
from process.enricher import Enricher
from process.merger import Merger
from process.transformer import Transformer
from process.loader import Loader

from utils.state_manager import State, RedisStorage

from config import DSL

from pprint import pprint

from psycopg2.extras import DictCursor
import psycopg2

from redis import Redis
from elasticsearch import Elasticsearch


@contextmanager
def closing(conn):
    try:
        yield conn
    finally:
        conn.close()


if __name__ == "__main__":
    while True:
        try:
            with closing(psycopg2.connect(**DSL, cursor_factory=DictCursor)) as pg_conn:
                redis_storage = RedisStorage(Redis(host='redis', port=6379))
                state_manager = State(redis_storage)
                producer = Producer(pg_conn, state_manager)
                enricher = Enricher(pg_conn, producer.get_extracted_persons(), state_manager)
                merger = Merger(pg_conn, enricher.get_extracted_film_works())
                transformer = Transformer(pg_conn, merger.get_extracted_merged_data())
                loader = Loader(Elasticsearch("http://elasticsearch:9200"), transformer.get_transformed_data())
                print('loop')
                time.sleep(10)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            time.sleep(5)
