import json
import pytest


class TestPersonApi:
    @pytest.yield_fixture(scope='class')
    async def create_person_index(self, es_client, person_index, person_load_data, redis_client):
        await redis_client.flushall()
        # создание индекса
        await es_client.indices.create(index='person', body=person_index, ignore=400)
        # заполнение индекса данными
        await es_client.bulk(body=person_load_data, index='person')
        movies_items = {}
        while not movies_items.get('count'):
            # ожидание пока данные в индекс загрузятся
            movies_items = await es_client.count(index='person')
        yield es_client
        await es_client.indices.delete(index='person', ignore=[400, 404])

    @pytest.fixture(scope='class')
    def person_index(self):
        file = 'tests/functional/testdata/indexes/person.json'
        with open(file) as f:
            return json.load(f)

    @pytest.fixture(scope='class')
    def person_load_data(self):
        file = 'tests/functional/testdata/load_data/person.json'
        with open(file) as f:
            return json.load(f)

    @pytest.fixture
    def all_persons(self):
        file = 'tests/functional/testdata/responses/all_persons.json'
        with open(file) as f:
            return json.load(f)

    @pytest.fixture
    def lucas(self, all_persons):
        return all_persons[0]

    @pytest.mark.asyncio
    async def test_one_person(self, make_get_request, lucas, create_person_index):
        v = make_get_request
        # Выполнение запроса
        response = await v('/person/3b31b4d1-6ab9-4552-b1ab-de3b61e25e24')
        assert response.status == 200
        assert response.body == lucas

    @pytest.mark.asyncio
    async def test_no_person(self, make_get_request, create_person_index):
        v = make_get_request
        # Выполнение запроса
        response = await v('/person/3b31b4d1-6ab9-4552-b1ab-de3b61e25e23')
        assert response.status == 404
        assert response.body == {'detail': "Person wasn't found"}

    @pytest.mark.asyncio
    async def test_person_films(self, make_get_request, create_person_index):
        v = make_get_request
        # Выполнение запроса
        response = await v('/person/3b31b4d1-6ab9-4552-b1ab-de3b61e25e24/film')
        assert response.status == 200
        assert response.body == [{'id': 'ab2811a3-3295-4564-988d-1ebc2ee03ab6',
                                  'imdb_rating': 8.6,
                                  'title': 'Star Wars: Episode IV - A New Hope'}]
