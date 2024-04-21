import logging
from psycopg2.extensions import connection as _connection

from .executor import QueryExecutor


class PostgresExtractor(QueryExecutor):
    def __init__(self, connection: _connection):
        super().__init__(connection)
        
    def extract_movies(self, query, vars: dict = None):
        for row in self.execute_query(query, vars=vars):
            yield row