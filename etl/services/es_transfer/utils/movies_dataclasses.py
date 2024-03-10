import uuid

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Filmwork:
    id: uuid.UUID
    title: str
    description: str
    creation_date: datetime
    file_path: str
    rating: float
    type: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class Genre:
    id: uuid.UUID
    name: str
    description: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class GenreFilmwork:
    id: uuid.UUID
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    created_at: datetime


@dataclass(frozen=True)
class Person:
    id: uuid.UUID
    full_name: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class PersonFilmwork:
    id: uuid.UUID
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str
    created_at: datetime
