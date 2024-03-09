import os

from dotenv import load_dotenv

load_dotenv()

DSL = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT"),
}

RUN_TESTS = os.environ.get('RUN_TRANSFER_DATA_FROM_SQLITE_TESTS', False) == 'True'
RUN_TRANSFER = os.environ.get('RUN_TRANSFER_DATA_FROM_SQLITE', False) == 'True'
