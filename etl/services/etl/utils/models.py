from datetime import datetime
from enum import Enum
from typing import List, Optional
import uuid
from pydantic import BaseModel


class PersonRolesEnum(Enum):
    ACTOR = 'actor'
    DIRECTOR = 'director'
    WRITER = 'writer'

class Person(BaseModel):
    person_id: str
    person_name: str
    person_role: str

class Movie(BaseModel):
    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    rating: Optional[float] = None
    type: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    persons: List[Person]
    genres: List[str]
    
class TransformedPerson(BaseModel):
    id: str
    full_name: str
    
class TransformedMovie(BaseModel):
    id: str
    imdb_rating: Optional[float]
    genres: List[str]
    title: str
    description: Optional[str]
    directors: Optional[List[TransformedPerson]]
    actors_names: Optional[List[str]]
    writers_names: Optional[List[str]]
    actors: Optional[List[TransformedPerson]]
    writers: Optional[List[TransformedPerson]]

def filter_persons(persons: List[Person], role: PersonRolesEnum) -> List[TransformedPerson]:
    return [TransformedPerson(id=person.person_id, full_name=person.person_name) for person in persons if person.person_role == role.value]
