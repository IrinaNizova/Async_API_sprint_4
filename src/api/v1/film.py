import json
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Request
from typing import List

from models.film import Film, FilterParams, ResponseMessage
from services.film import FilmService, get_film_service

router = APIRouter()


@router.get('/search', response_model=List[Film], response_model_include={'id', 'title', 'imdb_rating'})
async def search_film_list(
        request: Request,
        film_service: FilmService = Depends(get_film_service)
) -> List[Film]:
    _filters: FilterParams = request.state.filter_params
    pagination_params = _filters.dict(include={'sort', 'size', 'from_'})
    if _filters.query:
        body = json.dumps({"query": {"query_string": {"query": _filters.query,  "fuzziness": "auto"}}})
    else:
        body = None

    films = await film_service.get_all_with_filter(body=body, params=pagination_params)
    return films


# внедряем FilmService с помощью Depends(get_film_service)
@router.get('/{film_id}', response_model=Film, responses={
        404: {
            'model': ResponseMessage,
            'description': 'Film wasn\'t found'
        },
    })
async def film_details(
        film_id: str,
        film_service: FilmService = Depends(get_film_service)
):
    film = await film_service.get_by_id(film_id)
    if not film:
        # если фильм не найден то отдаем 404 статус
        # желательно пользоваться уже определенными enum содержащие HTTP статусы. Такой код будет более поддерживаемым
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    # перекладываем данные из models.Film в Film
    # обратите внимание, что модель бизнес логики имеет поле description которое отсутствует в модели ответа API.
    # Если бы использовалась общая модель для бизнес логики и формирования ответов API, то мы предоставляли клиентам
    # данные которые им не нужны и возможно данные которые опасно возвращать.
    return film


@router.get('/', response_model=List[Film], response_model_include={'id', 'title', 'imdb_rating'})
async def film_list(
        request: Request,
        film_service: FilmService = Depends(get_film_service),
        genre: str = None
) -> List[Film]:
    _filters: FilterParams = request.state.filter_params
    pagination_params = _filters.dict(include={'sort', 'size', 'from_'})

    body = json.dumps({"query": {"match": {"genre": {"query": genre, "fuzziness": "auto"}}}}) if genre else None
    films = await film_service.get_all_with_filter(body=body, params=pagination_params)
    return films

