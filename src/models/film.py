from typing import Optional, Union

import orjson

# используем pydantic для упрощения работы при перегонке данных из json в объекты
from pydantic import BaseModel, validator

class OrJsonConfig:

    class Config:
        # заменяем стандартную работу с json на более быструю
        json_loads = orjson.loads
        json_dumps = orjson.dumps


class Film(BaseModel, OrJsonConfig):
    id: str
    title: str
    genre: list
    actors: list
    writers: list
    imdb_rating: float = None
    description: str = None


class Genre(BaseModel, OrJsonConfig):
    id: str
    name: str
    films: str = None


class Person(BaseModel, OrJsonConfig):
    id: str
    full_name: str
    films_as_actor: Optional[str]
    films_as_writer: Optional[str]
    films_as_director: Optional[str]

class ResponseMessage(BaseModel, OrJsonConfig):
    message: str = None


class FilterParams(BaseModel, OrJsonConfig):
    query: Optional[str]
    sort: Optional[str]
    from_: Union[None, str, int]
    page: Union[None, str, int]
    size: Union[None, str, int]

    @validator('page')
    def validate_from_(cls, v: str):
        if not v or not v.isnumeric() or int(v) == 0:
            return 0
        return int(v)

    @validator('size')
    def validate_size(cls, v: str):
        if not v or not v.isnumeric() or int(v) == 0:
            return 20
        return int(v)

    def calculate_offset_from_(self):
        self.from_ = self.size * (self.page - 1) if self.page > 0 else 0
