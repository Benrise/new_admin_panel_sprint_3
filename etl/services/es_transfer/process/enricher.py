import logging

from psycopg2 import DatabaseError
from psycopg2.extensions import connection as _connection


class Enricher:
    def __init__(self, pg_conn: _connection, person_ids):
        self.conn = pg_conn
        self.person_ids = person_ids

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

    def get_extracted_film_works(self):
        query = """
        SELECT fw.id, fw.updated_at
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        WHERE pfw.person_id IN %(person_ids)s
        ORDER BY fw.updated_at
        LIMIT 100;
        """
        params = {"person_ids": self.person_ids}
        records = self._execute_query(query, params)
        return tuple(records)
