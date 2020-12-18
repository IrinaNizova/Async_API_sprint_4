import configparser
import logging
import os
from functools import wraps
from time import sleep

import backoff
import elasticsearch
import psycopg2
from contextlib import ContextDecorator
from dotenv import load_dotenv
from psycopg2.extras import DictCursor
from redis import Redis

from extractor.psql_extractor import PsqlExtractor
from loader.es_loader import ESLoader
from state_storage.base_storage import BaseStorage
from state_storage.redis_storage import RedisStorage
from transformer.transormer import Transformer


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn

    return inner


def lookup_pg_max_time():
    return int(os.getenv('PG_MAX_TRIES'))


class PgConnector(ContextDecorator):
    def __init__(self, dsl):
        self.dsl = dsl
        self.connect = None

    def __enter__(self):
        try:
            connect = self.connect_to_pg()
        except psycopg2.OperationalError as e:
            logging.error('Postgresql is not available')
            raise psycopg2.OperationalError(e)
        else:
            self.connect = connect
            return connect

    def __exit__(self, *exc):
        self.connect.close()

    @backoff.on_exception(backoff.expo,
                          psycopg2.OperationalError,
                          max_time=lookup_pg_max_time)
    def connect_to_pg(self):
        return psycopg2.connect(**self.dsl, cursor_factory=DictCursor)


class ETL:
    def __init__(
            self,
            extractor: PsqlExtractor,
            transformer: Transformer,
            loader: ESLoader,
            limit,
            periodic
    ):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.limit = limit
        self.periodic = periodic

    def run(self):
        loader = self.loader.load()
        transformer = self.transformer.transform(loader)
        while True:
            self.extractor.extract(transformer, limit=self.limit)
            sleep(self.periodic)


if __name__ == '__main__':
    log_format = '%(asctime)s %(levelname)s: %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_format)
    logging.info('Started %s', __name__)

    load_dotenv()

    config = configparser.ConfigParser()
    config.read('settings.ini')

    limit = int(os.getenv('LIMIT'))
    periodic_start = int(os.getenv('PERIODIC_START'))

    dsl = {
        'dbname': os.getenv('PG_NAME'),
        'user': os.getenv('PG_USER'),
        'host': os.getenv('PG_HOST'),
        'port': os.getenv('PG_PORT'),
        'password': os.getenv('PG_PASSWORD')
    }

    with PgConnector(dsl) as pg_conn:
        redis_storage: BaseStorage = RedisStorage(
            redis_adapter=Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'))
        )

        redis_storage.save_state({'can_start_ETL': 'True'})

        extractor = PsqlExtractor(psql_connect=pg_conn, limit=limit, storage=redis_storage)
        loader = ESLoader(
            es_connect=elasticsearch.Elasticsearch(hosts=[os.getenv('ES_HOST')]),
            storage=redis_storage
        )
        etl_process = ETL(
            extractor=extractor,
            transformer=Transformer(),
            loader=loader,
            limit=limit,
            periodic=periodic_start
        )

        etl_process.run()
