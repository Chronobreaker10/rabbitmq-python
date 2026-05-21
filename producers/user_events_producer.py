import asyncio
import logging
import time
from random import sample

from aio_pika import Message

from rabbit.common.topic_user_events import TopicUserEventsRabbit
from rabbit.config import configure_logging

log = logging.getLogger(__name__)


async def main():
    async with TopicUserEventsRabbit() as rabbit:
        await rabbit.declare_user_registered_queue()
        await rabbit.declare_user_verified_queue()
        log.info("Connecting to RabbitMQ %s", rabbit.connection)
        log.info("Created channel %s", rabbit.channel)
        for i in range(5):
            message_body = f"User {i} registered at {time.strftime('%H:%M:%S')}"
            await rabbit.exchange.publish(
                Message(body=message_body.encode('utf-8')),
                routing_key=f"users.{i}.registered",
            )
            log.info("Produce message: %s", message_body)
            await asyncio.sleep(1)
        await asyncio.sleep(2)
        verify_indexes = sample(range(5), k=3)
        for i in verify_indexes:
            message_body = f"User {i} verified at {time.strftime('%H:%M:%S')}"
            await rabbit.exchange.publish(
                Message(body=message_body.encode('utf-8')),
                routing_key=f"users.{i}.verified",
            )
            log.info("Produce message: %s", message_body)
            await asyncio.sleep(1)

if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Bye!")
