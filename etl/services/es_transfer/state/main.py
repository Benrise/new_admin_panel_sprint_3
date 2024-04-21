import abc
from typing import Any, Dict
import json
from redis import Redis


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        pass


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


class RedisStorage(BaseStorage):

    def __init__(self, redis: Redis):
        self._redis = redis

    def save_state(self, state: Dict[str, Any]) -> None:
        r = self._redis
        p_data = json.dumps(state)
        r.set('data', p_data)

    def retrieve_state(self) -> Dict[str, Any]:
        r = self._redis
        p_data = r.get('data')
        if (p_data is not None):
            data = json.loads(p_data)
            return data
        return {}


class State:
    state = {}

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        self.state[key] = value
        self.storage.save_state(self.state)

    def get_state(self, key: str) -> Any:
        return self.state.get(key)
