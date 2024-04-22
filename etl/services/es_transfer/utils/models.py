from datetime import datetime
from typing import List, Optional
import uuid
from pydantic import BaseModel


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
    person_id: str
    person_name: str
    
class TransformedMovie(BaseModel):
    id: str
    imdb_rating: Optional[float]
    genres: List[str]
    title: str
    description: Optional[str]
    directors: Optional[List[str]]
    actors_names: Optional[List[str]]
    writers_names: Optional[List[str]]
    actors: Optional[List[TransformedPerson]]
    writers: Optional[List[TransformedPerson]]

