from functools import lru_cache
from typing import Any

from fastapi import Depends

from core.config import MONGO_NOTIFICATIONS_COLLECTION_NAME, AUTH_DB_DSN
from db.mongodb import AbstractDBAdapter, get_mongo
from models.notification import Notification
from services.base_services.object_service import BaseService
import aiopg


class NotificationService(BaseService):
    def __init__(self,
                 db_adapter: AbstractDBAdapter,
                 collection_name: str,
                 model: Any,
                 dsn: str):
        super().__init__(db_adapter=db_adapter, collection_name=collection_name, model=model)
        self.dsn = dsn

    async def get_objects(self, count: int = 15, page: int = 1, **kwargs):
        pg_conn = await aiopg.connect(dsn=self.dsn)
        cur = await pg_conn.cursor()
        await cur.execute(f"SELECT u.email"
                          "FROM User u"
                          f"WHERE u.login = {kwargs['user_login']} ")

        email = await cur.fetchone()
        notifications = await self.db_adapter.get_objects_from_db(
            self.model, {'email': email[0]}, self.collection_name
        )
        return notifications


@lru_cache()
def get_notification_service(
    db_adapter: AbstractDBAdapter = Depends(get_mongo),
) -> NotificationService:
    return NotificationService(
        dsn=AUTH_DB_DSN,
        db_adapter=db_adapter,
        model=Notification,
        collection_name=MONGO_NOTIFICATIONS_COLLECTION_NAME,
    )
