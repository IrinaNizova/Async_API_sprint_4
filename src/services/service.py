from typing import Optional, TypeVar

from aioredis import Redis
from elasticsearch import AsyncElasticsearch, exceptions
from pydantic import BaseModel

from db.redis import RedisCacheExecutor

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут
T = TypeVar('T', bound=BaseModel)


class Service:
    model = BaseModel
    index = None

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic
        self.redis_executor = RedisCacheExecutor(self.redis, self.model)

    async def get_by_id(self, item_id: str) -> Optional[T]:
        """
        Возвращает объект из кэша или эластика по id
        """
        # пытаемся получить данные из кеша ибо получение данных из кеша работает быстрее;
        item = await self.redis_executor.item_from_cache(item_id)
        if not item:
            # если  нет в кеше то ищем его в эластике
            item = await self.get_from_elastic(item_id)
            if not item:
                # если он отсутствует в эластике, значит отсутствует
                return None
            # сохраняем фильм  в кеш
            await self.redis_executor.put_item_to_cache(item)

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
        items = await self.redis_executor.items_from_cache(key_for_redis)
        if not items:
            items = await self.elastic.search(index=self.index, body=body, params=params)
            models = [self.model(**hit['_source']) for hit in items['hits']['hits']]
            await self.redis_executor.put_items_to_cache(models, key=key_for_redis)
            return models
        return items

    async def get_from_elastic(self, item_id: str) -> Optional[model]:
        try:
            doc = await self.elastic.get(self.index, item_id)
        except exceptions.NotFoundError:
            return {}
        return self.model(**doc['_source'])

