import backoff
import uvicorn
import sentry_sdk
from aio_pika import connect
from fastapi import FastAPI, Request
from fastapi.responses import ORJSONResponse
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

from api.v1 import events, notifications
from core import config
from services import rabbit_producer
from tracer import tracer

app = FastAPI(
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    title='Cервис нотификации для онлайн-кинотеатра',
    description='Сервис для получения событий и отправки информационных сообщений клиентам',
    version='1.0.0',
    default_response_class=ORJSONResponse,
)

sentry_sdk.init(dsn=config.SENTRY_DSN)


@app.on_event('startup')
@backoff.on_exception(backoff.expo, ConnectionError)
async def startup():
    rabbit_producer.connection = await connect(config.RABBIT_URL)


@app.on_event('shutdown')
async def shutdown():
    await rabbit_producer.connection.close()


@app.middleware('http')
async def add_tracing(request: Request, call_next):
    request_id = request.headers.get('X-Request-Id')
    if not request_id:
        raise RuntimeError('request id is required')
    response = await call_next(request)
    with tracer.start_span(request.url.path) as span:
        request_id = request.headers.get('X-Request-Id')
        span.set_tag('http.request_id', request_id)
        span.set_tag('http.url', request.url)
        span.set_tag('http.method', request.method)
        span.set_tag('http.status_code', response.status_code)
    return response


app.include_router(events.router, prefix='/api/v1/events', tags=['events'])
app.include_router(notifications.router, prefix='/api/v1/notifications', tags=['notifications'])
app.add_middleware(SentryAsgiMiddleware)

if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host='0.0.0.0',
        port=8000,
    )
