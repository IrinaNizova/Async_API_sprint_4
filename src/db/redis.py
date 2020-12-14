from aioredis import Redis

redis: Redis = None


# функция понадобится при внедрении зависимостей
async def get_redis() -> Redis:
    return redis
