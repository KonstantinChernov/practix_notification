import logging
import os
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from auth_grpc.auth_check import check_permission
from services.rabbit_producer import RabbitProducerAdapter, get_rabbit_producer

router = APIRouter()

logging.basicConfig(level=logging.INFO)


class CustomEmailEvent(BaseModel):
    receivers_emails: list[str]
    title: str
    text: str


@router.post(
    '/custom-email',
    summary='Точка для создания ивента на кастомное оповещение пользователей',
    description='Принимает событие и список адресов на которые необходимо отправить оповещение',
    response_description='возвращается статус код',
    tags=['views'],
)
@check_permission(roles=['Admin'])
async def send_custom_email_event(
        request: Request,
        event: CustomEmailEvent,
        rabbit_service: RabbitProducerAdapter = Depends(get_rabbit_producer),
):
    event_info = event.dict()
    request_id_header = request.headers.get('X-Request-Id')
    try:
        await rabbit_service.send_event(
            event_code=os.getenv('CUSTOM_EMAIL_QUEUE'),
            context=event_info,
            headers={'request_id': request_id_header}
        )
    except Exception as e:
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=f'{e}')
    return {'status': 'success'}
