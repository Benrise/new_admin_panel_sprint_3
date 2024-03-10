import logging

from psycopg2 import DatabaseError
from psycopg2.extensions import connection as _connection


class Merger:
    def __init__(self, pg_conn: _connection, film_works_ids):
        self.conn = pg_conn
        self.film_works_ids = film_works_ids

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

    def get_extracted_merged_data(self):
        query = """
        SELECT
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating as imdb_rating,
            fw.type,
            fw.created_at,
            fw.updated_at,
            pfw.role,
            p.id,
            p.full_name,
            g.name as genre
        FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id IN %(film_works_ids)s;
        """
        params = {"film_works_ids": self.film_works_ids}
        records = self._execute_query(query, params)
        return records
