import logging

from psycopg2 import DatabaseError
from psycopg2.extensions import connection as _connection

class Producer:
    def __init__(self, pg_conn: _connection):
        self.conn = pg_conn

    def _execute_query(self, query,  params=None):
        records = []
        try:
            curs = self.conn.cursor()
            curs.execute(query, params)
            while True:
                rows = curs.fetchmany(20)
                if rows:
                    for row in rows:
                        records.append(row[0])
                else:
                    break
            return records
        except DatabaseError as e:
            logging.error(f"Database error while insert data: {e}")
            curs.connection.rollback()

    def get_extracted_persons(self):
        query = """
        SELECT id
        FROM content.person
        LIMIT 100;
        """
        records = self._execute_query(query)
        return tuple(records)
