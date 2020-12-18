import logging
import os

import backoff
import elasticsearch
from elasticsearch.helpers import bulk

from ETL import coroutine
from loader.base_loader import BaseLoader
from state_storage.base_storage import BaseStorage
from indexes import PERSON_MAPPING, GENRE_MAPPING
from models import State


def lookup_es_max_time():
    return int(os.getenv('ES_MAX_TRIES'))


class ESLoader(BaseLoader):
    def __init__(self, es_connect: elasticsearch.Elasticsearch, storage: BaseStorage):
        self.connect = es_connect
        self.storage = storage

    @coroutine
    def load(self):
        while data_to_load := (yield):
            pure_data, last_update = data_to_load

            try:
                self.is_available_service()
                self.connect.indices.create(index='genre', body=GENRE_MAPPING, ignore=400)
                self.connect.indices.create(index='person', body=PERSON_MAPPING, ignore=400)
                errors = self.load_bulk(pure_data)
            except elasticsearch.exceptions.ConnectionError:
                logging.error('Elasticsearch is not available')
            else:
                if errors[1]:
                    logging.error('This films was\'n load into Elasticsearch: \n'.format(';\n'.join(errors[1])))
                logging.info('Loader send {} rows into Elasticsearch'.format(errors[0]))
                self.storage.save_state(State(last_update=last_update.isoformat(), can_start_ETL='True').dict())

    def load_bulk(self, pure_data):
        return bulk(self.connect, pure_data)

    @backoff.on_exception(backoff.expo,
                          Exception,
                          max_time=lookup_es_max_time)
    def is_available_service(self):
        self.connect.ping()
