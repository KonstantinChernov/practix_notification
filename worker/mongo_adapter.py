from abc import abstractmethod
from typing import Any

import backoff
from motor.motor_asyncio import AsyncIOMotorClient
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
    database_name = config('MONGO_NOTIFICATIONS_DB_NAME')

    def __init__(self, mongo: AsyncIOMotorClient):
        self.mongo_client = mongo

    @backoff.on_exception(backoff.expo, ConnectionError)
    async def get_objects_from_db(
        self, model: Any, query: dict, collection_name: str
    ) -> list:

        collection = getattr(self.mongo_client, self.database_name).get_collection(
            collection_name
        )
        data = []
        async for obj in collection.find(query):
            obj_id = str(obj['_id'])
            data.append(model(**obj, id=obj_id))
        return data

    @backoff.on_exception(backoff.expo, ConnectionError)
    async def get_object_from_db(self, model: Any, query: dict, collection_name: str):

        collection = getattr(self.mongo_client, self.database_name).get_collection(
            collection_name
        )
        obj = await collection.find_one(query)
        if obj:
            obj_id = str(obj['_id'])
            return model(**obj, id=obj_id)
        return None

    @backoff.on_exception(backoff.expo, ConnectionError)
    async def add_object_to_db(self, obj: dict, collection_name: str):

        collection = getattr(self.mongo_client, self.database_name).get_collection(
            collection_name
        )
        await collection.insert_one(obj)


