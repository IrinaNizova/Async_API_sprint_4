from abc import ABC, abstractclassmethod
from typing import Optional, List
from pydantic import BaseModel


class AbstractCacheExecutor(ABC):

    @abstractclassmethod
    def item_from_cache(self, item_id: str) -> Optional[BaseModel]:
        ...

    @abstractclassmethod
    def items_from_cache(self, item_id: str) -> Optional[List[BaseModel]]:
        ...

    @abstractclassmethod
    def put_item_to_cache(self, item: BaseModel) -> None:
        ...

    @abstractclassmethod
    def put_items_to_cache(self, item) -> None:
        ...
