import asyncio
import logging
import string
import time
from random import choices

from aio_pika import Message

from rabbit.common.email_updates import EmailUpdatesRabbit
from rabbit.config import configure_logging

log = logging.getLogger(__name__)


class UpdateEmailProducer(EmailUpdatesRabbit):
    async def produce_message(self, index: int) -> None:
        username = "".join(choices(string.ascii_letters, k=10))
        message_body = f"[{index:02d}] User {username} change email! {time.time()}"
        log.debug("Send message to RabbitMQ %s", message_body)
        await self.exchange.publish(
            Message(body=message_body.encode('utf-8')),
            routing_key="",
        )
        log.warning("Published message to RabbitMQ %s", message_body)


async def main():
    async with UpdateEmailProducer() as producer:
        await producer.declare_email_updates_exchange()
        log.info("Created channel %s", producer.channel)
        for i in range(10):
            await producer.produce_message(index=i)
            await asyncio.sleep(0.5)

if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Bye!")
