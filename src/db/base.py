from abc import ABC
from typing import Optional, List
from pydantic import BaseModel


class AbstractCacheExecutor(ABC):

    def item_from_cache(self, item_id: str) -> Optional[BaseModel]:
        ...

    def items_from_cache(self, item_id: str) -> Optional[List[BaseModel]]:
        ...

    def put_item_to_cache(self, item: BaseModel) -> None:
        ...

    def put_items_to_cache(self, item) -> None:
        ...
