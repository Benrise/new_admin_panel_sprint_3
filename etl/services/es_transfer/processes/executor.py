import logging
from psycopg2.extensions import connection as _connection


class QueryExecutor:
    def __init__(self, connection: _connection):
        self.connection = connection
        
    def execute_query(self, query, count=100, vars: dict = None):
        try:
            curs = self.connection.cursor()
            curs.execute(query, vars)
            while True:
                rows = curs.fetchmany(count)
                if rows:
                    return rows
                else:
                    break
        except self.connection.Error as e:
            logging.error(f"SQLite error while executing the query: {e}")
            raise