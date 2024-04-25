import os

from dotenv import load_dotenv

from utils.es_schema import index_settings, index_mappings

load_dotenv()

BACKOFF = {
    "start_sleep_time": os.environ.get("BACKOFF_START_SLEEP_TIME", 1),
    "factor": os.environ.get("BACKOFF_FACTOR", 2),
    "border_sleep_time": os.environ.get("BACKOFF_BORDER_SLEEP_TIME", 10),
}

ETL = {
    "sleep_time": os.environ.get("ETL_SLEEP_TIME", 1)
}

DSL = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT"),
}

ES = {
    "hosts": f"http://{os.environ.get('ES_HOST', '127.0.0.1')}:{os.environ.get('ES_PORT', 9200)}",
    "index_name": "movies",
    "index_settings": index_settings,
    "index_mappings": index_mappings 
}