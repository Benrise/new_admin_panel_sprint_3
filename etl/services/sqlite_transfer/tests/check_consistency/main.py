import os
import sqlite3
from contextlib import contextmanager
from enum import Enum
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

load_dotenv()

DSL = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT"),
}


@contextmanager
def closing(conn):
    try:
        yield conn
    finally:
        conn.close()


class TablesNamesEnum(Enum):
    FILM_WORK = "film_work"
    GENRE = "genre"
    GENRE_FILM_WORK = "genre_film_work"
    PERSON = "person"
    PERSON_FILM_WORK = "person_film_work"


def compare_dbs(connection: sqlite3.Connection, pg_conn: _connection):
    msqlite_tables = get_msqlite_target_tables(connection)
    postgres_tables = get_postgres_target_tables(pg_conn)
    print("-" * 24)

    assert check_equal_number_of_tables(
        msqlite_tables, postgres_tables
    ), """[Test 1][Fail] Unequal number of tables"""
    assert is_equal_fields_count(
        connection, pg_conn, msqlite_tables, postgres_tables
    ), "[Test 2][Fail] Unequal fields count in tables"
    assert check_fields_consistency(
        connection, pg_conn, msqlite_tables, postgres_tables
    ), "[Test 3][Fail] Invalid fields consistency"
    print("-" * 24)
    print("[Success] All tests passed!")


def check_equal_number_of_tables(msqlite_tables: list, postgres_tables: list):
    is_equal_fields_count = len(msqlite_tables) == len(postgres_tables)
    if is_equal_fields_count:
        print("[Test 1][Success] Equal number of tables passed!")
    return is_equal_fields_count


def get_msqlite_target_tables(conn: sqlite3.Connection):
    query = "SELECT * FROM sqlite_master WHERE type='table';"
    curs = conn.cursor()
    curs.execute(query)
    data = curs.fetchall()
    tables = [table[1] for table in data]
    return tables


def get_postgres_target_tables(conn: _connection):
    query = """SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'content';"""
    curs = conn.cursor()
    curs.execute(query)
    all_tables = []
    while True:
        rows = curs.fetchmany(20)
        if rows:
            for row in rows:
                all_tables.append(row)
        else:
            break
    target_tables = [
        target_table[0]
        for target_table in all_tables
        if target_table[0] in [movie_table.value for movie_table in TablesNamesEnum]
    ]
    return target_tables


def is_equal_fields_count(
    sqlite_conn: sqlite3.Connection,
    postgres_conn: _connection,
    msqlite_tables: list[str],
    postgres_tables: list[str],
):
    sqlite_tables_fields = []
    postgres_tables_fields = []

    for table in msqlite_tables:
        query = f"SELECT * FROM {table}"
        curs = sqlite_conn.cursor()
        curs.execute(query)
        data = curs.fetchall()
        sqlite_tables_fields.append({"table_name": table, "fields": [len(data)]})

    for table in postgres_tables:
        query = f"SELECT * FROM content.{table}"
        curs = postgres_conn.cursor()
        curs.execute(query)
        data = curs.fetchall()
        postgres_tables_fields.append({"table_name": table, "fields": [len(data)]})

    are_equal = all(
        item in postgres_tables_fields for item in sqlite_tables_fields
    ) and all(item in sqlite_tables_fields for item in postgres_tables_fields)
    if are_equal:
        print("[Test 2][Success] Equal fields count in tables passed!")
    return are_equal


def check_fields_consistency(
    sqlite_conn: sqlite3.Connection,
    postgres_conn: _connection,
    msqlite_tables: list[str],
    postgres_tables: list[str],
):
    sqlite_tables_fields = []
    postgres_tables_fields = []
    flag = True
    for table in msqlite_tables:
        query = f"SELECT id FROM {table}"
        curs = sqlite_conn.cursor()
        curs.execute(query)
        records = []
        while True:
            rows = curs.fetchmany(20)
            if rows:
                for row in rows:
                    records.append(row)
            else:
                break
        sqlite_tables_fields.append({"table_name": table, "fields": records})

    for table in postgres_tables:
        query = f"SELECT id FROM content.{table}"
        curs = postgres_conn.cursor()
        curs.execute(query)
        records = []
        while True:
            rows = curs.fetchmany(20)
            if rows:
                for row in rows:
                    records.append(row)
            else:
                break
        postgres_tables_fields.append({"table_name": table, "fields": records})

    for sqlite_table_fields in sqlite_tables_fields:
        query = f"SELECT id FROM content.{sqlite_table_fields['table_name']}"
        curs = postgres_conn.cursor()
        curs.execute(query)
        records = []
        while True:
            rows = curs.fetchmany(20)
            if rows:
                for row in rows:
                    records.append(row)
            else:
                break
        for x in records:
            if not (x[0] in [y[0] for y in sqlite_table_fields["fields"]]):
                flag = False
                return flag
    if flag:
        print("[Test 3][Success] Data consistency passed!")
    return flag


if __name__ == "__main__":
    with sqlite3.connect("db.sqlite") as sqlite_conn, closing(
        psycopg2.connect(**DSL, cursor_factory=DictCursor)
    ) as pg_conn:
        compare_dbs(sqlite_conn, pg_conn)
