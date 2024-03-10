from contextlib import contextmanager
import json

from process.producer import Producer
from process.enricher import Enricher
from process.merger import Merger
from process.transformer import Transformer
from process.loader import Loader

from config import DSL

from pprint import pprint

from psycopg2.extras import DictCursor
import psycopg2

from elasticsearch import Elasticsearch


@contextmanager
def closing(conn):
    try:
        yield conn
    finally:
        conn.close()


if __name__ == "__main__":
    with closing(psycopg2.connect(**DSL, cursor_factory=DictCursor)) as pg_conn:
        producer = Producer(pg_conn)
        enricher = Enricher(pg_conn, producer.get_extracted_persons())
        merger = Merger(pg_conn, enricher.get_extracted_film_works())
        transformer = Transformer(pg_conn, merger.get_extracted_merged_data())
        loader = Loader(Elasticsearch("http://localhost:9200"), transformer.get_transformed_data())
