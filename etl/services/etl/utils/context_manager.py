from contextlib import contextmanager


@contextmanager
def closing(conn):
    try:
        yield conn
    finally:
        conn.close()