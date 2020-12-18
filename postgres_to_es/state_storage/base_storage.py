from abc import ABC, abstractmethod


class BaseStorage(ABC):
    @abstractmethod
    def save_state(self, state: dict):
        ...

    @abstractmethod
    def retrieve_state(self) -> dict:
        ...
