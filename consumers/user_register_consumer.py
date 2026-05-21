import asyncio
import logging
import time

from aio_pika.abc import AbstractIncomingMessage

from rabbit import config
from rabbit.config import configure_logging
from rabbit.common.topic_user_events import TopicUserEventsRabbit

log = logging.getLogger(__name__)


async def process_user_register_event(msg: AbstractIncomingMessage) -> None:
    try:
        user_id: int = -1
        if msg.routing_key:
            user_id = int(msg.routing_key.split(".")[1])
        if user_id < 0:
            log.error("Error getting user ID for msg: %s", msg)
            return
        log.info(f"User [{user_id}] registered in app: %r", msg.body)
        await msg.ack()
    except ValueError as _:
        log.error("Error getting user ID for msg: %s", msg)


async def main():
    async with TopicUserEventsRabbit() as rabbit:
        log.info("Created channel %s", rabbit.channel)
        await rabbit.consume_messages(
            process_message_callback=process_user_register_event,
            queue_name=config.MQ_USER_EVENTS_REGISTERED_QUEUE,
        )
        await asyncio.Event().wait()


if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Bye!")
