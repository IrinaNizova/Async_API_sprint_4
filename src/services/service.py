from typing import Optional, TypeVar, List

from aioredis import Redis
from elasticsearch import AsyncElasticsearch, exceptions

import json
from pydantic import BaseModel

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут
T = TypeVar('T', bound=BaseModel)


class Service:
    model = BaseModel
    index = None

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, item_id: str) -> Optional[T]:
        """
        Возвращает объект из кэша или эластика по id
        """
        # пытаемся получить данные из кеша ибо получение данных из кеша работает быстрее;
        item = await self.item_from_cache(item_id)
        if not item:
            # если  нет в кеше то ищем его в эластике
            item = await self.get_from_elastic(item_id)
            if not item:
                # если он отсутствует в эластике, значит отсутствует
                return None
            # сохраняем фильм  в кеш
            await self._put_item_to_cache(item)

        return item

    @staticmethod
    def prepare_params_for_search(params: dict) -> dict:
        """
        Подготовка параметров поиска в Elasticsearch
        """
        es_params = {}
        if not params:
            return es_params
        for key, value in params.items():
            if value:
                if key == 'number':
                    es_params['from'] = (params['number'] - 1) * (params.get('size') or 10)
                elif key == 'sort':
                    order = 'desc' if value.startswith('-') else 'asc'
                    es_params['sort'] = ":".join((value.strip('-'), order))
                else:
                    es_params[key] = value
        return es_params

    async def get_all_with_filter(self, body=None, params=None) -> list:
        return await self.get_all_from_elastic(body=body, params=self.prepare_params_for_search(params))

    async def get_all_from_elastic(self, body=None, params=None) -> list:
        key_for_redis = "_".join((self.index, ";".join((f"{key}={value}" for key, value in params.items())), str(body)))
        items = await self.items_from_cache(key_for_redis)
        if not items:
            items = await self.elastic.search(index=self.index, body=body, params=params)
            models = [self.model(**hit['_source']) for hit in items['hits']['hits']]
            await self._put_items_to_cache(models, key=key_for_redis)
            return models
        return items

    async def get_from_elastic(self, item_id: str) -> Optional[model]:
        try:
            doc = await self.elastic.get(self.index, item_id)
        except exceptions.NotFoundError:
            return {}
        return self.model(**doc['_source'])

    async def item_from_cache(self, item_id: str) -> Optional[model]:
        # пытаемся получить данные о фильме из кеша использую команду get
        # https://redis.io/commands/get
        data = await self.redis.get(item_id)
        if not data:
            return None

        # pydantic предоставляет удобное API для создания объекта моделей из json
        item = self.model.parse_raw(data)
        return item

    async def items_from_cache(self, item_id: str) -> Optional[List[model]]:
        data = await self.redis.get(item_id)
        if not data:
            return None
        else:
            return [self.model.parse_raw(item) for item in json.loads(data)]

    async def _put_item_to_cache(self, item: Model):
        # сохраняем данные о фильме с использованием команды set
        # выставляем время жизни кеша равное 5 минутам
        # https://redis.io/commands/set
        # pydantic позволяет модель сериализовать в json
        await self.redis.set(item.id, item.json(), expire=FILM_CACHE_EXPIRE_IN_SECONDS)

    async def _put_items_to_cache(self, items: List[Optional[T]], key=None):
        # сохраняем данные о фильме с использованием команды set
        # выставляем время жизни кеша равное 5 минутам
        # https://redis.io/commands/set
        # pydantic позволяет модель сериализовать в json
        values = [item.json() for item in items]
        await self.redis.set(key, json.dumps(values), expire=FILM_CACHE_EXPIRE_IN_SECONDS)
