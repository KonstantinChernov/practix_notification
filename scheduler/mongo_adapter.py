from abc import abstractmethod
from typing import Any

import backoff
from pymongo import MongoClient
from decouple import config


class AbstractDBAdapter:
    @abstractmethod
    async def get_objects_from_db(self, *args):
        pass

    @abstractmethod
    async def get_object_from_db(self, *args):
        pass

    @abstractmethod
    async def add_object_to_db(self, *args):
        pass

    @abstractmethod
    async def delete_object_from_db(self, *args):
        pass

    @abstractmethod
    async def update_object_from_db(self, *args):
        pass


class MongoAdapter(AbstractDBAdapter):
    database_name = config('MONGO_DB_NAME')

    def __init__(self, mongo: MongoClient):
        self.mongo_client = mongo

    @backoff.on_exception(backoff.expo, ConnectionError)
    def get_objects_from_db(
        self, model: Any, query: dict, collection_name: str
    ) -> list:

        collection = getattr(getattr(self.mongo_client, self.database_name), collection_name)
        data = []
        for obj in collection.find(query):
            obj_id = str(obj['_id'])
            data.append(model(**obj, id=obj_id))
        return data
