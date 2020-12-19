from aioredis import Redis

from typing import Optional, TypeVar, List
import json

from pydantic import BaseModel

from db.base import AbstractCacheExecutor

redis: Redis = None

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут
T = TypeVar('T', bound=BaseModel)


# функция понадобится при внедрении зависимостей
async def get_redis() -> Redis:
    return redis


class RedisCacheExecutor(AbstractCacheExecutor):
    model = BaseModel

    def __init__(self, redis, model):
        self.redis = redis
        self.model = model

    async def item_from_cache(self, item_id: str):
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

    async def put_item_to_cache(self, item: Optional[T]) -> None:
        # сохраняем данные о фильме с использованием команды set
        # выставляем время жизни кеша равное 5 минутам
        # https://redis.io/commands/set
        # pydantic позволяет модель сериализовать в json
        await self.redis.set(item.id, item.json(), expire=FILM_CACHE_EXPIRE_IN_SECONDS)

    async def put_items_to_cache(self, items: List[Optional[T]], key=None) -> None:
        # сохраняем данные о фильме с использованием команды set
        # выставляем время жизни кеша равное 5 минутам
        # https://redis.io/commands/set
        # pydantic позволяет модель сериализовать в json
        values = [item.json() for item in items]
        await self.redis.set(key, json.dumps(values), expire=FILM_CACHE_EXPIRE_IN_SECONDS)
