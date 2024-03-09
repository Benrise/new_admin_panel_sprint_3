import logging
import sqlite3
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass, fields

import psycopg2
from config import (
    DSL,
    RUN_TRANSFER,
    RUN_TESTS
)
from psycopg2 import DatabaseError
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from utils.movies_dataclasses import (
    Filmwork,
    Genre,
    GenreFilmwork,
    Person,
    PersonFilmwork,
)

table_name_model_mapping = {
    "film_work": Filmwork,
    "genre": Genre,
    "genre_film_work": GenreFilmwork,
    "person": Person,
    "person_film_work": PersonFilmwork,
}



@contextmanager
def closing(conn):
    try:
        yield conn
    finally:
        conn.close()


@dataclass
class DataTable:
    name: str = None
    data: list = None


@dataclass
class DataTables:
    film_work: DataTable
    genre: DataTable
    genre_film_work: DataTable
    person: DataTable
    person_film_work: DataTable


class SQLiteExtractor:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    def _execute_query(self, query):
        records = []
        try:
            curs = self.connection.cursor()
            curs.execute(query)
            while True:
                rows = curs.fetchmany(20)
                if rows:
                    for row in rows:
                        records.append(row)
                else:
                    break
            return records
        except sqlite3.Error as e:
            logging.error(f"SQLite error while executing the query: {e}")
            raise

    def _get_tables(self):
        query = "SELECT * FROM sqlite_master WHERE type='table';"
        return self._execute_query(query)

    def _create_records(self, rows, record_class):
        records = []
        for row in rows:
            try:
                record = record_class(*row)
                records.append(record)
            except Exception as e:
                logging.error(f"Error on creating a record: {e}")
                raise
        return records

    def extract_movies(self):
        data_tables = DataTables(
            DataTable(),
            DataTable(),
            DataTable(),
            DataTable(),
            DataTable(),
        )
        try:
            tables = self._get_tables()
            for table in tables:
                table_name = table[1]
                query = f"SELECT * FROM {table_name}"
                rows = self._execute_query(query)
                data_class_model = table_name_model_mapping[table_name]
                data_class_data = self._create_records(rows, data_class_model)
                setattr(data_tables, table_name, DataTable(table_name, data_class_data))
            return data_tables

        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise


class PostgresSaver:
    def __init__(self, connection: _connection):
        self.connection = connection

    def save_all_data(self, data_tables: DataTables):
        curs = self.connection.cursor()
        for _, data_table in data_tables.__dict__.items():
            if isinstance(data_table, DataTable) and data_table.data:
                insert_table_records(data_table, curs)


def insert_table_records(data_table: DataTable, curs: DictCursor):
    try:
        args = []
        attribute_names = []
        for item in data_table.data:
            item_fields = fields(item)
            attribute_names = [field.name for field in item_fields]
            values = [getattr(item, field) for field in attribute_names]
            args.append(
                curs.mogrify(
                    f"({', '.join(['%s']*len(attribute_names))})", tuple(values)
                ).decode()
            )
        args_str = ",".join(args)
        attribute_names_str = ", ".join(attribute_names)

        curs.execute(
            f"""
            INSERT INTO content.{data_table.name} ({attribute_names_str})
            VALUES {args_str}
            ON CONFLICT (id) DO NOTHING
        """
        )

        curs.connection.commit()

    except DatabaseError as e:
        logging.error(f"Database error while insert data: {e}")
        curs.connection.rollback()

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    data = sqlite_extractor.extract_movies()
    postgres_saver.save_all_data(data)


if __name__ == "__main__":
    if RUN_TRANSFER:
        with sqlite3.connect("db.sqlite") as sqlite_conn, closing(
            psycopg2.connect(**DSL, cursor_factory=DictCursor)
        ) as pg_conn:
            print("Data transfer has started...")
            load_from_sqlite(sqlite_conn, pg_conn)
            print("Data transfer is completed!")
        if RUN_TESTS:
            try:
                print("Running tests...")
                subprocess.run(["python3", "tests/check_consistency/main.py"])
            except Exception as e:
                logging.error(f"Error on running tests: {e}")
    else:
        print("Data transfer was skipped...")
