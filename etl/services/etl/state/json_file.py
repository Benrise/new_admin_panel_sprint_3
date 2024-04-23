from typing import Any, Dict
import json
from .base import BaseStorage


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        with open(self.file_path, "w") as write_file:
            json.dump(state, write_file)

    def retrieve_state(self) -> Dict[str, Any]:
        try:
            with open(self.file_path, "r") as read_file:
                data = json.load(read_file)
                return data

        except FileNotFoundError:
            return {}

