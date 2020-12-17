from http import HTTPStatus
from typing import List, Optional

from fastapi import Depends, HTTPException, APIRouter, Query, Request

from models.film import ResponseMessage, Genre, FilterParams
from services.genre import GenreService, get_genre_service


router = APIRouter()


@router.get(
    '/',
    summary="Список жанров",
    description="Список жанров фильмов",
    response_description="Список жанров и фильмов с ними",
    response_model=List[Genre],
)
async def get_genres(
        request: Request,
        genre_service: GenreService = Depends(get_genre_service),
) -> List[Genre]:
    _filters: FilterParams = request.state.filter_params
    pagination_params = _filters.dict(include={'sort', 'size', 'from_'})

    genres: List[Genre] = await genre_service.get_all_with_filter(params=pagination_params)
    return genres


@router.get(
    '/{genre_uuid}',
    summary="Информация по жанру",
    description="Подробная информация по uuid жанра",
    response_description="Жанр и фильмы с ним",
    response_model=Genre,
    responses={
        404: {
            'model': ResponseMessage,
            'description': 'Genre wasn\'t found'
        },
    }
)
async def get_genre(
        genre_service: GenreService = Depends(get_genre_service),
        genre_uuid: str = None
) -> Genre:
    genre: Genre = await genre_service.get_by_id(genre_uuid)
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Person wasn\'t found')
    return genre