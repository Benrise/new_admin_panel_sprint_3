from psycopg2 import DatabaseError
from psycopg2.extensions import connection as _connection
from utils.es_dataclasses import Movie

import dataclasses

import logging


class Transformer:
    def __init__(self, pg_conn: _connection, extracted_data,):
        self.conn = pg_conn
        self.extracted_data = extracted_data

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

    def get_roles_for_filmwork(self, film_work_id, role_type):
        query = """
        SELECT
            p.id,
            p.full_name
        FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
        WHERE fw.id = %(film_work_id)s
            AND pfw.role = %(role_type)s;
        """
        params = {
            "film_work_id": film_work_id,
            "role_type": role_type
        }
        records = self._execute_query(query, params)
        return [{"id": record[0], "full_name": record[1]} for record in records]

    def get_genres_for_filmwork(self, film_work_id):
        query = """
        SELECT
            g.name
        FROM content.film_work fw
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id = %(film_work_id)s;
        """
        params = {
            "film_work_id": film_work_id,
        }
        records = self._execute_query(query, params)
        return [record[0] for record in records]

    def get_transformed_data(self):
        movies: Movie = []
        for record in self.extracted_data:
            movie = Movie(
                id=record['id'],
                imdb_rating=record['imdb_rating'],
                genres=self.get_genres_for_filmwork(record['fw_id']),
                title=record['title'],
                description=record['description'],
                directors=self.get_roles_for_filmwork(record['fw_id'], 'director'),
                actors_names=[actor['full_name'] for actor in self.get_roles_for_filmwork(record['fw_id'], 'actor')],
                writers_names=[actor['full_name'] for actor in self.get_roles_for_filmwork(record['fw_id'], 'writer')],
                actors=self.get_roles_for_filmwork(record['fw_id'], 'actor'),
                writers=self.get_roles_for_filmwork(record['fw_id'], 'writer')
            )
            movies.append(dataclasses.asdict(movie))
        return movies
