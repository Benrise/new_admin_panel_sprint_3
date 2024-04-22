import abc
from typing import Any, Dict


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        pass