import logging
import os
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Request

from auth_grpc.auth_check import check_permission
from models.event import Event
from services.rabbit_producer import RabbitProducerAdapter, get_rabbit_producer

router = APIRouter()

logging.basicConfig(level=logging.INFO)


@router.post(
    '/custom-notification',
    summary='Точка для создания ивента на кастомное оповещение пользователей',
    description='Принимает событие и список адресов на которые необходимо отправить оповещение',
    response_description='возвращается статус код',
    tags=['events'],
)
@check_permission(roles=['Admin'])
async def send_custom_notification_event(
        request: Request,
        event: Event,
        rabbit_service: RabbitProducerAdapter = Depends(get_rabbit_producer),
):
    event_info = event.dict()
    request_id_header = request.headers.get('X-Request-Id')
    try:
        await rabbit_service.send_event(
            event_code=os.getenv('CUSTOM_NOTIFICATION_QUEUE'),
            context=event_info,
            headers={'request_id': request_id_header}
        )
    except Exception as e:
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=f'{e}')
    return {'status': 'success'}
