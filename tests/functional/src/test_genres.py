import json
import pytest


class TestGenreApi:

    @pytest.yield_fixture(scope='class')
    async def create_genre_index(self, es_client, genre_index, genre_load_data, redis_client):
        await redis_client.flushall()
        # создание индекса
        await es_client.indices.create(index='genre', body=genre_index, ignore=400)
        # заполнение индекса данными
        await es_client.bulk(body=genre_load_data, index='genre')
        movies_items = {}
        while not movies_items.get('count'):
            # ожидание пока данные в индекс загрузятся
            movies_items = await es_client.count(index='genre')
        yield es_client
        await es_client.indices.delete(index='genre', ignore=[400, 404])

    @pytest.fixture(scope='class')
    def genre_index(self):
        file = 'tests/functional/testdata/indexes/genre.json'
        with open(file) as f:
            return json.load(f)

    @pytest.fixture(scope='class')
    def genre_load_data(self):
        file = 'tests/functional/testdata/load_data/genres.json'
        with open(file) as f:
            return json.load(f)

    @pytest.fixture
    def all_genres(self):
        file = 'tests/functional/testdata/responses/all_genres.json'
        with open(file) as f:
            return json.load(f)

    @pytest.mark.asyncio
    async def test_all_genres(self, make_get_request, create_genre_index, all_genres):
        v = make_get_request
        # Выполнение запроса
        response = await v('/genre')
        assert response.status == 200
        assert response.body == all_genres

    @pytest.mark.asyncio
    async def test_one_genre(self, make_get_request, create_genre_index, all_genres):
        v = make_get_request
        # Выполнение запроса
        response = await v('/genre/ba3f980c-645e-4fb3-afc1-d57d2b0f3d87')
        assert response.status == 200
        assert response.body == all_genres[1]

