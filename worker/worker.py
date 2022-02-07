import asyncio
import json
import os
import backoff
from aio_pika import connect, IncomingMessage
from motor.motor_asyncio import AsyncIOMotorClient
from mongo_adapter import MongoAdapter

import sentry_sdk

from handlers import EventHandler

loop = asyncio.get_event_loop()

sentry_sdk.init(
    os.getenv('SENTRY_DSN'),
    traces_sample_rate=1.0
)


async def on_user_registration(message: IncomingMessage):
    async with message.process():
        mongo_client = AsyncIOMotorClient(os.getenv('MONGO_NOTIFICATIONS_HOST'), os.getenv('MONGO_NOTIFICATIONS_PORT'))
        input_mongo_adapter = MongoAdapter(mongo=mongo_client)
        request_id = message.headers['request_id']
        info = json.loads(message.body.decode())

        email = info.pop('email')
        info['receivers_emails'] = [email]

        handler = EventHandler(info=info,
                               request_id=request_id,
                               event_type=os.getenv('USER_REGISTRATION_QUEUE'),
                               mongo_adapter=input_mongo_adapter)
        await handler.run()


async def on_custom_email_event(message: IncomingMessage):
    async with message.process():
        mongo_client = AsyncIOMotorClient(os.getenv('MONGO_NOTIFICATIONS_HOST'), os.getenv('MONGO_NOTIFICATIONS_PORT'))
        input_mongo_adapter = MongoAdapter(mongo=mongo_client)
        request_id = message.headers['request_id']
        info = json.loads(message.body.decode())

        handler = EventHandler(info=info,
                               request_id=request_id,
                               event_type=os.getenv('CUSTOM_NOTIFICATION_QUEUE'),
                               mongo_adapter=input_mongo_adapter)
        await handler.run()


async def on_common_week_event(message: IncomingMessage):
    async with message.process():
        mongo_client = AsyncIOMotorClient(os.getenv('MONGO_NOTIFICATIONS_HOST'), os.getenv('MONGO_NOTIFICATIONS_PORT'))
        input_mongo_adapter = MongoAdapter(mongo=mongo_client)
        request_id = message.headers['request_id']
        info = json.loads(message.body.decode())

        handler = EventHandler(info=info,
                               request_id=request_id,
                               event_type=os.getenv('COMMON_WEEK_QUEUE'),
                               mongo_adapter=input_mongo_adapter)
        await handler.run()


async def on_personal_week_event(message: IncomingMessage, mongo_adapter: MongoAdapter):
    async with message.process():
        mongo_client = AsyncIOMotorClient(os.getenv('MONGO_NOTIFICATIONS_HOST'), os.getenv('MONGO_NOTIFICATIONS_PORT'))
        input_mongo_adapter = MongoAdapter(mongo=mongo_client)
        request_id = message.headers['request_id']
        info = json.loads(message.body.decode())

        email = info.pop('email')
        info['receivers_emails'] = [email]

        handler = EventHandler(info=info,
                               request_id=request_id,
                               event_type=os.getenv('PERSONAL_WEEK_QUEUE'),
                               mongo_adapter=input_mongo_adapter)
        await handler.run()


@backoff.on_exception(backoff.expo, ConnectionError)
async def event_listener(queue_name):
    callbacks = {
        os.getenv('USER_REGISTRATION_QUEUE'): on_user_registration,
        os.getenv('CUSTOM_NOTIFICATION_QUEUE'): on_custom_email_event,
        os.getenv('COMMON_WEEK_QUEUE'): on_common_week_event,
        os.getenv('PERSONAL_WEEK_QUEUE'): on_personal_week_event,
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
    loop.create_task(event_listener(queue_name=os.getenv('USER_REGISTRATION_QUEUE')))
    loop.create_task(event_listener(queue_name=os.getenv('CUSTOM_NOTIFICATION_QUEUE')))
    loop.create_task(event_listener(queue_name=os.getenv('COMMON_WEEK_QUEUE')))
    loop.create_task(event_listener(queue_name=os.getenv('PERSONAL_WEEK_QUEUE')))

    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()
