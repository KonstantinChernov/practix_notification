from typing import Any

from db.mongodb import AbstractDBAdapter


class BaseService:
    def __init__(self, db_adapter: AbstractDBAdapter, collection_name: str, model: Any):
        self.db_adapter = db_adapter
        self.model = model
        self.collection_name = collection_name

    async def get_objects(self, count: int = 15, page: int = 1, **kwargs):
        objects = await self.db_adapter.get_objects_from_db(
            self.model, kwargs, self.collection_name
        )
        return objects
