import logging

from extractor.base_extractor import BaseExtractor
from state_storage.base_storage import BaseStorage
from SQL_scripts import CREATE_TMP_LAST_UPDATED_FILMS, FIND_UPDATED_GENRES, FIND_UPDATED_PERSONS, FIND_UPDATED_FILMS, \
    CREATE_TMP_FILM_GENRES, CREATE_TMP_FILM_PERSONS, CLEAR_DATA_OVER_LIMIT, GET_UPDATED_FILMS_INFO, \
    GET_UPDATED_GENRES_INFO, GET_UPDATED_PERSONS_INFO, DROP_TMP_TABLES
from models import State


class PsqlExtractor(BaseExtractor):

    def __init__(self, psql_connect, limit, storage: BaseStorage):
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
