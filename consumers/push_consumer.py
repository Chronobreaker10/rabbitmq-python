import asyncio
import logging
import time

from aio_pika.abc import AbstractIncomingMessage

from rabbit import config
from rabbit.config import configure_logging
from rabbit.common.email_updates import EmailUpdatesRabbit

log = logging.getLogger(__name__)


async def send_push(msg: AbstractIncomingMessage) -> None:
    try:
        log.info("[ ] Start sending push: %r", msg.body)
        start_time = time.perf_counter()
        await asyncio.sleep(1)
        end_time = time.perf_counter()
        await msg.ack()
        log.info("[X] Finish sending push %r in %.2fs", msg.body, end_time - start_time)
    except Exception as e:
        log.exception("Error sending push: %s", e)


async def main():
    async with EmailUpdatesRabbit() as rabbit:
        log.info("Created channel %s", rabbit.channel)
        await rabbit.consume_messages(
            process_message_callback=send_push,
            queue_name=config.MQ_EMAIL_UPDATES_PUSH_QUEUE_NAME,
        )
        await asyncio.Event().wait()


if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Bye!")
