import aiohttp
import aioredis
import asyncio
import json
import pytest
from elasticsearch import AsyncElasticsearch

from ..settings import ELASTIC_HOST, ELASTIC_PORT
from dataclasses import dataclass
from multidict import CIMultiDictProxy
from ..settings import SERVICE_URL, REDIS_HOST, REDIS_PORT


@dataclass
class HTTPResponse:
    body: dict
    headers: CIMultiDictProxy[str]
    status: int


@pytest.yield_fixture(scope='session')
def event_loop():
    # Настройка
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    # Очистка
    loop.close()


@pytest.fixture
def loop(event_loop):
    return event_loop


@pytest.fixture(scope='session')
async def es_client():
    client = AsyncElasticsearch(hosts=[f'{ELASTIC_HOST}:{ELASTIC_PORT}'])
    yield client
    await client.close()


@pytest.fixture(scope='session')
async def redis_client():
    redis = await aioredis.create_redis_pool((REDIS_HOST, REDIS_PORT), minsize=10, maxsize=20)
    yield redis
    redis.close()


@pytest.fixture(scope='session')
async def session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest.fixture
def make_get_request(session):
    async def inner(method: str, params: dict = None) -> HTTPResponse:
        params = params or {}
        url = SERVICE_URL + '/api/v1' + method  # в боевых системах старайтесь так не делать!
        async with session.get(url, params=params) as response:
            return HTTPResponse(
                body=await response.json(),
                headers=response.headers,
                status=response.status,
            )

    return inner


@pytest.yield_fixture(scope='session')
async def create_movie_index(es_client, create_film_index, film_load_data, redis_client):
    await redis_client.flushall()
    # создание индекса
    await es_client.indices.create(index='movies', body=create_film_index, ignore=400)
    # заполнение индекса данными
    await es_client.bulk(body=film_load_data, index='movies')
    movies_items = {}
    while not movies_items.get('count'):
        # ожидание пока данные в индекс загрузятся
        movies_items = await es_client.count(index='movies')
    yield es_client
    await es_client.indices.delete(index='movies', ignore=[400, 404])


@pytest.fixture(scope='session')
def create_film_index():
    file = 'tests/functional/testdata/indexes/film.json'
    with open(file) as f:
        return json.load(f)

@pytest.fixture(scope='session')
def film_load_data():
    file = 'tests/functional/testdata/load_data/films.json'
    with open(file) as f:
        return json.load(f)
