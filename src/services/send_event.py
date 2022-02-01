import json
import asyncio
import os

from aio_pika import connect, Message, DeliveryMode


class EventSender:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    async def async_send_event(self, context, event_code):
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
            delivery_mode=DeliveryMode.PERSISTENT
        )
        await channel.default_exchange.publish(
            message,
            routing_key=event_code
        )

        await connection.close()

    def send_event(self, context, event_code):
        return self.loop.run_until_complete(self.async_send_event(context, event_code))
