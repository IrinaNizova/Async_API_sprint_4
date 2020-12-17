from abc import ABC, abstractmethod


class BaseLoader(ABC):
    @abstractmethod
    def load(self, state: dict):
        ...

    @abstractmethod
    def is_available_service(self) -> bool:
        ...
