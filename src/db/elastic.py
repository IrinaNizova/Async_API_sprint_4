from elasticsearch import AsyncElasticsearch, exceptions
from pydantic import BaseModel
from typing import Optional, List

es: AsyncElasticsearch = None


# функция понадобится при внедрении зависимостей
async def get_elastic() -> AsyncElasticsearch:
    return es


class ElasticExecutor:

    def __init__(self, elastic, index, model):
        self.elastic = elastic
        self.index = index
        self.model = model

    async def get_from_elastic_by_id(self, item_id: str) -> Optional[BaseModel]:
        try:
            doc = await self.elastic.get(self.index, item_id)
        except exceptions.NotFoundError:
            return {}
        return self.model(**doc['_source'])

    async def get_detected_from_elastic(self, body: dict, params: dict) -> Optional[List[BaseModel]]:
        items = await self.elastic.search(index=self.index, body=body, params=params)
        models = [self.model(**hit['_source']) for hit in items['hits']['hits']]
        return models
