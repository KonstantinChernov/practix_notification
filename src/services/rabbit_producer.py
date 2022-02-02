import json
from typing import Optional

import backoff as backoff
from aio_pika import Message, DeliveryMode, connect

connection: Optional[connect] = None


class RabbitProducerAdapter:
    def __init__(self, connection: connect):
        self.connection = connection

    @backoff.on_exception(backoff.expo, ConnectionError)
    async def send_event(self, context: dict, event_code: str, headers: dict = None):

        channel = await self.connection.channel()
        await channel.declare_queue(
            event_code,
            durable=True
        )

        message = Message(
            json.dumps(context).encode("utf-8"),
            delivery_mode=DeliveryMode.PERSISTENT,
            headers=headers
        )
        await channel.default_exchange.publish(
            message,
            routing_key=event_code
        )


async def get_rabbit_producer() -> RabbitProducerAdapter:
    return RabbitProducerAdapter(connection=connection)
