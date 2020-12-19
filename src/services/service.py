from typing import Optional, TypeVar

from aioredis import Redis
from elasticsearch import AsyncElasticsearch, exceptions
from pydantic import BaseModel

from db.elastic import ElasticExecutor
from db.redis import RedisCacheExecutor

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут
T = TypeVar('T', bound=BaseModel)


class Service:
    model = BaseModel
    index = None

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.elastic = elastic
        self.redis_executor = RedisCacheExecutor(redis, self.model)
        self.elastic_executor = ElasticExecutor(self.elastic, self.index, self.model)

    async def get_by_id(self, item_id: str) -> Optional[T]:
        """
        Возвращает объект из кэша или эластика по id
        """
        # пытаемся получить данные из кеша ибо получение данных из кеша работает быстрее;
        item = await self.redis_executor.item_from_cache(item_id)
        if not item:
            # если  нет в кеше то ищем его в эластике
            item = await self.elastic_executor.get_from_elastic_by_id(item_id)
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

    async def get_all_from_elastic(self, body=None, params=None) -> list:
        params = self.prepare_params_for_search(params)
        key_for_redis = "_".join((self.index, ";".join((f"{key}={value}" for key, value in params.items())), str(body)))
        items = await self.redis_executor.items_from_cache(key_for_redis)
        if not items:
            models = await self.elastic_executor.get_detected_from_elastic(body, params)
            await self.redis_executor.put_items_to_cache(models, key=key_for_redis)
            return models
        return items


