import datetime
import psycopg2
from psycopg2.extras import DictCursor

from utils.context_manager import closing
from processes.extractror import PostgresExtractor

from utils.sql_queries import sql_extract_query

from config import (
    DSL
)


if __name__ == "__main__":
    with closing(psycopg2.connect(**DSL, cursor_factory=DictCursor)) as pg_conn:
        extractor = PostgresExtractor(pg_conn)
        vars = {
            'updated_at': '2021-01-01 00:00:00',
            'limit': 100
        }
        movies=[]
        while True:
            movies.append(next(extractor.extract_movies(sql_extract_query, vars=vars)))
            print(len(movies))