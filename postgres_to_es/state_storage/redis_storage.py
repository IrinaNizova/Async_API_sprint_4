from redis import Redis

from state_storage.base_storage import BaseStorage


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter
        self.r_hash = 'postgresql_films'

    def save_state(self, state: dict) -> None:
        self.redis_adapter.hmset(self.r_hash, state)

    def retrieve_state(self) -> dict:
        data = self.redis_adapter.hgetall(self.r_hash)
        if not data:
            return {}
        return data