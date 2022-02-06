import uuid
from datetime import datetime
from typing import Any, List

from pydantic import BaseModel

from utils import CustomBaseModel


class FilmPerson(BaseModel):
    id: uuid.UUID
    name: str


class FilmGenre(BaseModel):
    id: uuid.UUID
    name: str


class Film(CustomBaseModel):
    id: uuid.UUID
    dt_create: datetime
    rating: float
    type: str
    title: str
    description: str = None
    genres_names: List[str] = None
    directors_names: List[str] = None
    actors_names: List[str] = None
    writers_names: List[str] = None
    genres: List[FilmGenre] = []
    directors: List[FilmPerson] = []
    actors: List[FilmPerson] = []
    writers: List[FilmPerson] = []


class Favorite(CustomBaseModel):
    id: Any
    film_id: uuid.UUID
    user_login: str
