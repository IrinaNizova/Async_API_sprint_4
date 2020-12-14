import json
import pytest


class TestFilmApi:

    @pytest.fixture
    def all_films(self):
        file = 'tests/functional/testdata/responses/all_films.json'
        with open(file) as f:
            return json.load(f)

    @pytest.fixture
    def new_hope_film(self):
        file = 'tests/functional/testdata/responses/new_hope.json'
        with open(file) as f:
            return json.load(f)


    @pytest.mark.asyncio
    @pytest.mark.parametrize(('page_number', 'page_size', 'sort_sign'), ((1, 5, ''), (1, 2, ''), (2, 2, ''), (1, 5, '-')))
    async def test_all_films(self, make_get_request, all_films,  page_size, page_number, sort_sign):
        if sort_sign == "-":
            all_films = all_films[::-1]
        v = make_get_request
        response = await v('/film', {'page[number]': page_number, 'page[size]': page_size, 'sort': sort_sign + 'imdb_rating'})

        assert response.status == 200
        assert len(response.body) == page_size
        assert response.body == all_films[(page_number-1)*page_size:page_number*page_size]

    @pytest.mark.asyncio
    async def test_one_film(self, es_client, redis_client, make_get_request, new_hope_film, create_film_index):
        v = make_get_request
        response = await v('/film/ab2811a3-3295-4564-988d-1ebc2ee03ab6')

        assert response.status == 200
        assert response.body == new_hope_film

    @pytest.mark.asyncio
    async def test_no_exist_film(self, es_client, redis_client, make_get_request):
        v = make_get_request
        response = await v('/film/ab2811a3-3295-4564-988d-1ebc2ee03ab7')

        assert response.status == 404

