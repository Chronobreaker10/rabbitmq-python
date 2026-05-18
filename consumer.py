import asyncio
import logging
import time
from random import random

from aio_pika.abc import AbstractRobustChannel, AbstractIncomingMessage

from rabbit.common.direct_notifications import DirectNotifyRabbit
from rabbit.config import configure_logging, MQ_NOTIFICATIONS_QUEUE
from rabbit import RabbitBase

log = logging.getLogger(__name__)


async def process_message(msg: AbstractIncomingMessage):
    try:
        log.info("[ ] Start processing message: %r", msg.body)
        index = int(msg.body[1:3])
        is_odd = index % 2
        delay = 3 if is_odd else 5
        start_time = time.perf_counter()
        await asyncio.sleep(delay)
        end_time = time.perf_counter()
        if random() < 0.3:
            raise Exception("Internal error ")
        # Подтверждаем обработку сообщения (оно исчезнет из очереди)
        await msg.ack()

        log.info("[X] Finish processing message %r in %.2fs", msg.body, end_time - start_time)
    except Exception as e:
        log.exception("Error processing message: %s", e)
        log.exception("Try retry process message:")
        # Возвращаем сообщение обратно в очередь для повторной обработки
        # Без вызова nack потребитель просто остановит работу
        # Нам придется явно перезапускать процесс
        await msg.nack(requeue=False)
        # await msg.nack(requeue=False) === msg.reject() - сообщение не вернется в очередь
        # await msg.nack(multiple=True) - вернет в очередь {prefetch_count} сообщений при ошибке обработки любого из них


async def main():
    async with DirectNotifyRabbit() as rabbit:
        log.info("Created channel %s", rabbit.channel)
        await rabbit.consume_messages(process_message_callback=process_message, prefetch_count=3)
        await asyncio.Event().wait()


if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Bye!")
