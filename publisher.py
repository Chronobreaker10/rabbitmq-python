import logging
import time

import aio_pika
from aio_pika import Message
from aio_pika.abc import AbstractRobustChannel

from rabbit.config import configure_logging, get_connection, MQ_NOTIFICATIONS_ROUTING_KEY, MQ_NOTIFICATIONS_EXCHANGE_NAME
import asyncio

log = logging.getLogger(__name__)


async def produce_message(channel: AbstractRobustChannel, index: int) -> None:
    message_body = f"[{index:02d}] Hello, World! {time.time()}"
    log.debug("Send message to RabbitMQ %s", message_body)
    # durable - обменник не исчезнет после остановки Rabbit
    exchange = await channel.declare_exchange(MQ_NOTIFICATIONS_EXCHANGE_NAME, aio_pika.ExchangeType.DIRECT, durable=True)
    queue = await channel.declare_queue(MQ_NOTIFICATIONS_ROUTING_KEY, durable=True)
    await queue.bind(exchange, routing_key=MQ_NOTIFICATIONS_ROUTING_KEY)
    await exchange.publish(
        # aio_pika.DeliveryMode.PERSISTENT - сообщение не исчезнет после остановки Rabbit
        Message(body=message_body.encode('utf-8'), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
        routing_key=MQ_NOTIFICATIONS_ROUTING_KEY,
    )
    log.warning("Published message to RabbitMQ %s", message_body)


async def main():
    async with get_connection() as conn:
        log.info("Connecting to RabbitMQ %s", conn)
        async with conn.channel() as channel:
            log.info("Created channel %s", conn)
            for i in range(10):
                await produce_message(channel, index=i)
                await asyncio.sleep(0.5)
            # while True:
            #     pass

if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Bye!")
