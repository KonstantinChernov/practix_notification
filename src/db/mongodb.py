from abc import abstractmethod
from typing import Any, Optional

import backoff
from motor.motor_asyncio import AsyncIOMotorClient

from core.config import MONGO_DB_NAME

mongo_client: Optional[AsyncIOMotorClient] = None


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
    database_name = MONGO_DB_NAME

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

    @backoff.on_exception(backoff.expo, ConnectionError)
    async def delete_object_from_db(self, query: dict, collection_name: str):

        collection = getattr(self.mongo_client, self.database_name).get_collection(
            collection_name
        )
        await collection.delete_one(query)

    @backoff.on_exception(backoff.expo, ConnectionError)
    async def update_object_from_db(
        self, query: dict, updated_fields: dict, collection_name: str
    ):

        collection = getattr(self.mongo_client, self.database_name).get_collection(
            collection_name
        )
        a = await collection.update_one(query, updated_fields)


async def get_mongo() -> MongoAdapter:
    return MongoAdapter(mongo=mongo_client)
