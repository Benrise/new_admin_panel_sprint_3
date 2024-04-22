from typing import Any, Dict
import json
from redis import Redis
from .base import BaseStorage

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