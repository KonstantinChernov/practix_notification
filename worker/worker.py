import asyncio
import json
import os
import backoff
from aio_pika import connect, IncomingMessage

import sentry_sdk

from handlers import EventHandler

loop = asyncio.get_event_loop()

sentry_sdk.init(
    os.getenv('SENTRY_DSN'),
    traces_sample_rate=1.0
)


async def on_user_registration(message: IncomingMessage):
    async with message.process():
        request_id = message.headers['request_id']
        info = json.loads(message.body.decode())

        email = info.pop('email')
        info['receivers_emails'] = [email]

        handler = EventHandler(info=info,
                               request_id=request_id,
                               event_type=os.getenv('USER_REGISTRATION_QUEUE'))
        await handler.run()


async def on_custom_email_event(message: IncomingMessage):
    async with message.process():
        request_id = message.headers['request_id']
        info = json.loads(message.body.decode())

        handler = EventHandler(info=info,
                               request_id=request_id,
                               event_type=os.getenv('CUSTOM_EMAIL_QUEUE'))
        await handler.run()


@backoff.on_exception(backoff.expo, ConnectionError)
async def event_listener(queue_name):
    callbacks = {
        os.getenv('USER_REGISTRATION_QUEUE'): on_user_registration,
        os.getenv('CUSTOM_EMAIL_QUEUE'): on_custom_email_event,
    }

    connection = await connect(f"amqp://{os.getenv('RABBITMQ_DEFAULT_USER')}:"
                               f"{os.getenv('RABBITMQ_DEFAULT_PASS')}@movies-rabbitmq:5672/")
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    queue = await channel.declare_queue(
        queue_name,
        durable=True
    )
    await queue.consume(callbacks[queue_name])


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(event_listener(os.getenv('USER_REGISTRATION_QUEUE')))
    loop.create_task(event_listener(os.getenv('CUSTOM_EMAIL_QUEUE')))

    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()
