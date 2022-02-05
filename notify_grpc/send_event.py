import json
import asyncio
import os

import backoff
from aio_pika import connect, Message, DeliveryMode


class EventSender:
    def __init__(self, loop):
        self.loop = loop
        asyncio.set_event_loop(self.loop)

    @backoff.on_exception(backoff.expo, ConnectionError)
    async def async_send_event(self, context: dict, event_code: str, request_id_header: str):
        connection = await connect(f"amqp://{os.getenv('RABBITMQ_DEFAULT_USER')}:"
                                   f"{os.getenv('RABBITMQ_DEFAULT_PASS')}@movies-rabbitmq:5672/",
                                   loop=self.loop)
        channel = await connection.channel()
        await channel.declare_queue(
            event_code,
            durable=True
        )

        message = Message(
            json.dumps(context).encode("utf-8"),
            delivery_mode=DeliveryMode.PERSISTENT,
            headers={'request_id': request_id_header},
            priority=os.getenv('MAX_PRIORITY_MESSAGE')
        )
        await channel.default_exchange.publish(
            message,
            routing_key=event_code
        )

        await connection.close()

    def send_event(self, context: dict, event_code: str, request_id_header: str):
        return self.loop.run_until_complete(self.async_send_event(context, event_code, request_id_header))
