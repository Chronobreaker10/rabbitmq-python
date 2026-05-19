import asyncio
import logging
import time

from rabbit import config
from rabbit.common.direct_notifications import DirectNotifyRabbit
from rabbit.config import configure_logging

log = logging.getLogger(__name__)


async def main():
    async with DirectNotifyRabbit(durable=True, retry=True) as rabbit:
        # await rabbit.declare_direct_notify_queue(ttl=config.RMQ_TTL_MS, with_dlq=True)
        await rabbit.declare_direct_notify_queue(with_dlq=True)
        log.info("Connecting to RabbitMQ %s", rabbit.connection)
        log.info("Created channel %s", rabbit.channel)
        for i in range(30):
            message_body = f"[{i:02d}] Hello, World! {time.strftime('%H:%M:%S')}"
            await rabbit.publish_message(message_body)
            await asyncio.sleep(0.1)


async def clear_rabbit():
    async with DirectNotifyRabbit() as rabbit:
        await rabbit.clear()


if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Bye!")
