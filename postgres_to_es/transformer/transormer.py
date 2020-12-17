import json
import logging

from ETL import coroutine
from models import FilmMap, GenreMap, PersonMap
from transformer.base_transormer import BaseTransformer


class Transformer(BaseTransformer):
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
