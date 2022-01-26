import uvicorn
import sentry_sdk
from fastapi import FastAPI, Request
from fastapi.responses import ORJSONResponse
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

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
async def startup():
    ...


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


app.add_middleware(SentryAsgiMiddleware)

if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host='0.0.0.0',
        port=8000,
    )
