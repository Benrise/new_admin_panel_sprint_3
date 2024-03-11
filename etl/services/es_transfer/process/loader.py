from elasticsearch import Elasticsearch
from utils.es_schemas import settings, mappings


class Loader():
    def __init__(self, es_conn: Elasticsearch, data):
        self.es_conn = es_conn
        self.es_create_index_if_not_exists(
            index="movies",
            settings=settings,
            mappings=mappings
        )
        self.es_conn.bulk(index='movies', body=self.gen_data(data))

    def es_create_index_if_not_exists(self, index, settings, mappings):
        if not self.es_conn.indices.exists(index=index):
            self.es_conn.indices.create(index=index, settings=settings, mappings=mappings)

    def gen_data(self, data: list):
        actions = []
        for doc in data:
            actions.append({"index": {"_index": "movies", "_id": doc['id']}})
            actions.append(doc)
        return actions
