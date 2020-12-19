import json
from http import HTTPStatus
from typing import List, Optional

from fastapi import Depends, HTTPException, APIRouter, Query, Request
from api.v1.film import Film

from models.film import Person, ResponseMessage, FilterParams
from services.film import FilmService, get_film_service
from services.person import PersonService, get_person_service


router = APIRouter()


@router.get('/search',
            summary="Поиск персон",
            description="Полнотекстовый поиск по персонам",
            response_description="ФИО персоны и фильмы, где она приняла участие",
            tags=['Полнотекстовый поиск'],
            response_model=List[Person])
async def search_person_list(
        request: Request,
        person_service: PersonService = Depends(get_person_service),
) -> List[Film]:
    _filters: FilterParams = request.state.filter_params
    pagination_params = _filters.dict(include={'sort', 'size', 'from_'})
    if _filters.query:
        body = json.dumps({"query": {"query_string": {"query": _filters.query,  "fuzziness": "auto"}}})
    else:
        body = None

    persons = await person_service.get_all_from_elastic(body=body, params=pagination_params)
    return persons


@router.get(
    '/{person_uuid}',
    summary="Информация по персоне",
    description="Подробная информация по uuid персоны",
    response_description="ФИО персоны и фильмы, где она приняла участие",
    response_model=Person,
    responses={
        404: {
            'model': ResponseMessage,
            'description': 'Person wasn\'t found'
        },
    }
)
async def get_person(
        person_service: PersonService = Depends(get_person_service),
        person_uuid: str = None
) -> Person:
    person: Person = await person_service.get_by_id(person_uuid)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Person wasn\'t found')
    return person


@router.get(
    '/{person_uuid}/film',
    summary="Информация по фильмам, где участвовала персона",
    description="Информация о фильмах, где приняла участие персона",
    response_description="Список фильмов, где приняла участие персона",
    response_model=List[Film],
    response_model_include={'id', 'title', 'imdb_rating'},
    responses={
        404: {
            'model': ResponseMessage,
            'description': 'Person wasn\'t found'
        },
    }
)
async def get_persons_films(
        request: Request,
        person_service: PersonService = Depends(get_person_service),
        film_service: FilmService = Depends(get_film_service),
        person_uuid: str = None,
) -> List[Film]:
    _filters: FilterParams = request.state.filter_params
    pagination_params = _filters.dict(include={'sort', 'size', 'from_'})

    person: Person = await person_service.get_by_id(person_uuid)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Person wasn\'t found')

    person_films = []
    for role_films in (person.films_as_actor, person.films_as_writer, person.films_as_director):
        if role_films:
            person_films.extend(role_films.split(","))

    body = json.dumps({"query": {"terms": {"_id": person_films}}})
    person_films = await film_service.get_all_from_elastic(body=body, params=pagination_params)
    return person_films
