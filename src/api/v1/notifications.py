import logging

from fastapi import APIRouter, Depends, Request
from fastapi.params import Query

from utils import get_user_login

from auth_grpc.auth_check import check_permission
from services.notifications import NotificationService, get_notification_service

router = APIRouter()

logging.basicConfig(level=logging.INFO)


@router.get(
    '/notifications',
    summary='Точка для получения ивента пользователя',
    response_description='Возвращает ивенты пользователя',
    tags=['notifications'],
)
@check_permission(roles=['Subscriber'])
async def get_user_notifications(
        request: Request,
        count: int = Query(20),
        page: int = Query(1),
        notification_service: NotificationService = Depends(get_notification_service),
):
    login = get_user_login(request)
    notifications = notification_service.get_objects(count=count, page=page, user_login=login)
    return {'status': 'success',
            'data': notifications}
