import configparser
import json
import logging
import os
from functools import wraps
from time import sleep

import backoff
import elasticsearch
import psycopg2
from contextlib import ContextDecorator
from dotenv import load_dotenv
from elasticsearch.helpers import bulk
from psycopg2.extras import DictCursor
from redis import Redis

from SQL_scripts import *
from indexes import PERSON_MAPPING
from models import State, FilmMap, GenreMap, PersonMap


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn

    return inner


def lookup_pg_max_time():
    return int(os.getenv('PG_MAX_TRIES'))


def lookup_es_max_time():
    return int(os.getenv('ES_MAX_TRIES'))


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


class RedisStorage:
    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter
        self.r_hash = 'postgresql_films'

    def save_state(self, state: dict) -> None:
        self.redis_adapter.hmset(self.r_hash, state)

    def retrieve_state(self) -> dict:
        data = self.redis_adapter.hgetall(self.r_hash)
        if not data:
            return {}
        return data


class PsqlExtractor:

    def __init__(self, psql_connect, limit, storage: RedisStorage):
        self.connect = psql_connect
        self.limit = limit
        self.storage = storage

    def extract(self, transformer, limit):
        logging.info('Start extractor')
        state = self.storage.retrieve_state()
        last_update = state['last_update'.encode()].decode() if state.get('last_update'.encode()) else '1970-01-01'
        if state['can_start_ETL'.encode()].decode() == 'True':
            state['can_start_ETL'.encode()] = 'False'
            self.storage.save_state(state)
        else:
            logging.info('Can\'t start ETL process, because ETL-process already in process'.format(last_update))
            return

        cur = self.connect.cursor()
        # find new updated films
        cur.execute(CREATE_TMP_LAST_UPDATED_FILMS)
        cur.execute(FIND_UPDATED_GENRES, (last_update,))
        cur.execute(FIND_UPDATED_PERSONS, (last_update,))
        cur.execute(FIND_UPDATED_FILMS, (last_update,))
        # collect films info (genres, persons)
        cur.execute(CREATE_TMP_FILM_GENRES, (limit,))
        cur.execute(CREATE_TMP_FILM_PERSONS, (limit,))
        cur.execute(CLEAR_DATA_OVER_LIMIT, (limit,))
        # get updated films
        cur.execute(GET_UPDATED_FILMS_INFO)
        last_updated_films = cur.fetchall()
        # get updated genres
        cur.execute(GET_UPDATED_GENRES_INFO)
        last_updated_genres = cur.fetchall()
        # get updated persons
        cur.execute(GET_UPDATED_PERSONS_INFO)
        last_updated_persons = cur.fetchall()

        cur.execute(DROP_TMP_TABLES)

        if not last_updated_films:
            logging.info(f'No new changes have been detected since {last_update}')
            self.storage.save_state(State(last_update=last_update, can_start_ETL='True').dict())
            return

        logging.info('Ectractor get {} rows to update'.format(
            len(last_updated_films) + len(last_updated_persons) + len(last_updated_genres))
        )

        updated_data = (last_updated_persons, last_updated_genres, last_updated_films)
        transformer.send(updated_data)


class Transformer:
    @coroutine
    def transform(self, loader):
        while raw_films_info := (yield):
            pure_data = []
            raw_persons, raw_genres, raw_films = raw_films_info
            self.transform_films(raw_films, pure_data)
            last_updated_film = raw_films[-1].get('last_update')
            self.transform_genres(raw_genres, pure_data)
            self.transform_persons(raw_persons, pure_data)

            loader.send((pure_data, last_updated_film))

    @staticmethod
    def transform_films(raw_films, pure_data: list):
        for film_info in raw_films:
            persons = json.loads(film_info['jsonify_persons'])
            actors = []
            writers = []
            director = None
            for person in persons:
                if person['role'] == 'Actor':
                    actors.append({'id': person['id'], 'name': person['name']})
                elif person['role'] == 'Writer':
                    writers.append({'id': person['id'], 'name': person['name']})
                elif person['role'] == 'Director':
                    director = person['name']

            film = FilmMap(
                _index='movies',
                _id=film_info['film_id'],
                id=film_info['film_id'],
                imdb_rating=float(film_info['rating']) if film_info['rating'] else None,
                genre=film_info['genre'].split(','),
                title=film_info['title'],
                description=film_info['description'],
                director=director,
                actors_names=[actor['name'] for actor in actors],
                writers_names=[writer['name'] for writer in writers],
                actors=actors,
                writers=writers,
            )
            pure_data.append(film.dict())
        logging.info('Transformer clear {} films to update'.format(len(raw_films)))

    @staticmethod
    def transform_genres(raw_genres, pure_data):
        for genre in raw_genres:
            genre_info = GenreMap(
                _index='genre',
                _id=genre['genre_id'],
                id=genre['genre_id'],
                name=genre['genre_name'],
                films=genre['genre_films']
            )
            pure_data.append(genre_info.dict())
        logging.info('Transformer clear {} genres to update'.format(len(raw_genres)))

    @staticmethod
    def transform_persons(raw_persons, pure_data):
        if not raw_persons:
            return

        dictable_pure_data = dict()

        for person in raw_persons:
            if not dictable_pure_data.get(person['person_id']):

                person_info = PersonMap(
                    _index='person',
                    _id=person['person_id'],
                    id=person['person_id'],
                    full_name=person['full_name'],
                    films_as_actor=None,
                    films_as_writer=None,
                    films_as_director=None,
                )

                dictable_pure_data[person['person_id']] = person_info

            if person['role'].lower() == 'actor':
                dictable_pure_data[person['person_id']].films_as_actor = person['person_films']
            if person['role'].lower() == 'writer':
                dictable_pure_data[person['person_id']].films_as_writer = person['person_films']
            if person['role'].lower() == 'director':
                dictable_pure_data[person['person_id']].films_as_director = person['person_films']

        for person_info in dictable_pure_data.values():
            pure_data.append(person_info.dict())
        logging.info('Transformer clear {} persons to update'.format(len(raw_persons)))


class ESLoader:
    def __init__(self, es_connect: elasticsearch.Elasticsearch, storage: RedisStorage):
        self.connect = es_connect
        self.storage = storage

    @coroutine
    def load(self):
        while data_to_load := (yield):
            pure_data, last_update = data_to_load

            try:
                self.check_es_available()
                if not self.connect.indices.exists(index='genre'):
                    self.create_genre_index()
                if not self.connect.indices.exists(index='person'):
                    self.create_person_index()
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
    def check_es_available(self):
        self.connect.ping()

    def create_genre_index(self):
        genre_mapping = {
            'mappings': {
                'properties': {
                    'id': {'type': 'keyword'},
                    'name': {'type': 'text'},
                    'films': {'type': 'text'}
                }
            }
        }
        self.connect.indices.create(index='genre', body=genre_mapping)

    def create_person_index(self):
        person_mapping = PERSON_MAPPING
        self.connect.indices.create(index='person', body=person_mapping)


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
        redis_storage = RedisStorage(redis_adapter=Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT')))

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
