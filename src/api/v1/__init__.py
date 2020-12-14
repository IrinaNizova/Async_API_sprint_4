from fastapi import APIRouter

from . import person, film, genre

router = APIRouter()

# теги указываем для удобства навигации по документации
router.include_router(film.router,            prefix='/film',            tags=['Фильмы'])
router.include_router(person.router,          prefix='/person',          tags=['Персоны'])
router.include_router(genre.router,           prefix='/genre',           tags=['Жанры'])
