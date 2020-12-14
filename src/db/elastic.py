from elasticsearch import AsyncElasticsearch

es: AsyncElasticsearch = None


# функция понадобится при внедрении зависимостей
async def get_elastic() -> AsyncElasticsearch:
    return es
