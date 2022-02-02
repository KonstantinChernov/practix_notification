import asyncio
import json
import os

import backoff
from aio_pika import connect, IncomingMessage

from sentry_sdk import capture_exception
import sentry_sdk

from handlers import UserRegistrationHandler
from tracer import tracer


loop = asyncio.get_event_loop()

sentry_sdk.init(
    os.getenv('SENTRY_DSN'),
    traces_sample_rate=1.0
)


async def on_user_registration(message: IncomingMessage):
    async with message.process():
        print(" [x] Received message %r" % message)
        print("     Message body is: %r" % message.body)
        request_id = message.headers['request_id'][0].decode()
        registered_user_info = json.loads(message.body.decode())
        handler = UserRegistrationHandler(info=registered_user_info)
        with tracer.start_span('start-send-user-welcome') as span:
            span.set_tag('http.request_id', request_id)
            try:
                handler.run()
                span.set_tag('response_from_notification_worker_handler', 'success')
            except Exception as e:
                capture_exception(e)
                span.set_tag('response_from_notification_worker_handler', f'Error: {e}')


@backoff.on_exception(backoff.expo, ConnectionError)
async def user_registration_listener():
    connection = await connect(f"amqp://{os.getenv('RABBITMQ_DEFAULT_USER')}:"
                               f"{os.getenv('RABBITMQ_DEFAULT_PASS')}@{os.getenv('RABBITMQ_HOST')}/")
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    queue = await channel.declare_queue(
        os.getenv('USER_REGISTRATION_QUEUE'),
        durable=True
    )
    await queue.consume(on_user_registration)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(user_registration_listener())

    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()
