import uuid

from typing import List, Optional
from dataclasses import dataclass


@dataclass(frozen=True)
class Person:
    id: uuid.UUID
    name: str


@dataclass(frozen=True)
class Movie:
    id: uuid.UUID
    imdb_rating: float
    genres: List[str]
    title: str
    description: str
    directors: List[str]
    actors_names: List[str]
    writers_names: List[str]
    actors: List[Person]
    writers: List[Person]
