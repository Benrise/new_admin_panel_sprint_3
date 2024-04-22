from datetime import datetime
from typing import List, Optional
import uuid
from pydantic import BaseModel


class Person(BaseModel):
    person_id: str
    person_name: str
    person_role: str

class Movie(BaseModel):
    id: Optional[uuid.UUID] = None
    title: Optional[str] = None
    description: Optional[str] = None
    rating: Optional[float] = None
    type: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    persons: List[Person]
    genres: List[str]

