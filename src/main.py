import logging

import aioredis
import uvicorn as uvicorn
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI, Request
from fastapi.responses import ORJSONResponse

import api
from core import config
from core.logger import LOGGING
from db import elastic, redis
from models.film import FilterParams

app = FastAPI(
    title=config.PROJECT_NAME,
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
    description="Информация о фильмах, жанрах и людях, участвовавших в создании произведения",
    version="1.0.0"
)


@app.on_event('startup')
async def startup():
    redis.redis = await aioredis.create_redis_pool((config.REDIS_HOST, config.REDIS_PORT), minsize=10, maxsize=20)
    elastic.es = AsyncElasticsearch(hosts=[f'{config.ELASTIC_HOST}:{config.ELASTIC_PORT}'])


@app.on_event('shutdown')
async def shutdown():
    await redis.redis.close()
    await elastic.es.close()


app.include_router(api.router, prefix='/api')


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    query = request.query_params.get('query')
    sort = request.query_params.get('sort')
    page = request.query_params.get('page[number]')
    size = request.query_params.get('page[size]')
    filter_params = FilterParams(
        query=query,
        sort=sort,
        size=size,
        page=page,
        from_=0
    )
    filter_params.calculate_offset_from_()
    # filter_params = dict(query=query, sort=sort, size=size, from_=size * (int(page) - 1) if int(page) > 0 else 0)
    request.state.filter_params = filter_params
    response = await call_next(request)
    # process_time = time.time() - start_time
    # response.headers["X-Process-Time"] = str(process_time)
    return response

if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host='0.0.0.0',
        port=8000,
        log_config=LOGGING,
        log_level=logging.DEBUG,
    )

