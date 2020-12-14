from typing import Optional, List, Dict

from pydantic.main import BaseModel, Extra


class State(BaseModel):
    last_update: str
    can_start_ETL: str


class FilmMap(BaseModel):
    _index: str
    _id: str
    id: str
    imdb_rating: Optional[float]
    genre: List[str]
    title: str
    description: Optional[str]
    director: Optional[str]
    actors_names: List[str]
    writers_names: List[str]
    actors: List[Dict]
    writers: List[Dict]

    class Config:
        extra = Extra.allow


class PersonMap(BaseModel):
    _index: str
    _id: str
    id: str
    full_name: str
    films_as_actor: Optional[str]
    films_as_writer: Optional[str]
    films_as_director: Optional[str]

    class Config:
        extra = Extra.allow


class GenreMap(BaseModel):
    _index: str
    _id: str
    id: str
    name: str
    films: str

    class Config:
        extra = Extra.allow
