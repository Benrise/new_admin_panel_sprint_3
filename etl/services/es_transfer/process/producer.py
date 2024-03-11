import datetime
import logging

from psycopg2 import DatabaseError
from psycopg2.extensions import connection as _connection
from utils.state_manager import State


class Producer:
    def __init__(self, pg_conn: _connection, state: State):
        self.conn = pg_conn
        self.state = state

    def _execute_query(self, query,  params=None):
        records = []
        try:
            curs = self.conn.cursor()
            curs.execute(query, params)
            while True:
                rows = curs.fetchmany(20)
                if rows:
                    for row in rows:
                        records.append(row)
                else:
                    break
            return records
        except DatabaseError as e:
            logging.error(f"Database error while insert data: {e}")
            curs.connection.rollback()

    def get_extracted_persons(self):
        query = """
        SELECT id, updated_at
        FROM content.person
        WHERE updated_at > %(updated_at)s
        LIMIT 100;
        """
        updated_at = self.state.get_state('person') or datetime.date.min
        params = {
            "updated_at": updated_at,
        }
        records = self._execute_query(query, params)
        if (records):
            updated_at = records[-1]['updated_at']
            self.state.set_state('person', str(updated_at.date()))
        return tuple([record[0] for record in records])
