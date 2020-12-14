from fastapi import APIRouter

from . import v1

router = APIRouter()

# подключаем роутер к серверу указав префикс /v1
router.include_router(v1.router, prefix='/v1')