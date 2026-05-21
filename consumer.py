from __future__ import annotations

import asyncio
import logging
import time
from random import random

from aio_pika.abc import AbstractIncomingMessage

from rabbit.common.direct_notifications import DirectNotifyRabbit
from rabbit.config import configure_logging, MQ_NOTIFICATIONS_EXCHANGE, MQ_FAILED_NOTIFICATIONS_QUEUE

log = logging.getLogger(__name__)


def extract_deaths_count(
        headers: dict[str, list[dict[str, int]]] | None
) -> int:
    if headers and headers.get("x-death"):
        for props in headers["x-death"]:
            if "count" in props:
                return int(props["count"])
    return 0


async def process_message(msg: AbstractIncomingMessage):
    deaths_count = extract_deaths_count(msg.headers)
    try:
        log.info("[⌛] Start processing message: %r, deaths %d", msg.body, deaths_count)
        index = int(msg.body[1:3])
        is_odd = index % 2
        delay = 3 if is_odd else 5
        start_time = time.perf_counter()
        await asyncio.sleep(0.5)
        end_time = time.perf_counter()
        if random() < 0.0001:
            raise Exception("Internal error ")
        # Подтверждаем обработку сообщения (оно исчезнет из очереди)
        await msg.ack()
        log.info("[✅] Finish processing message %r in %.2fs after %d retries", msg.body, end_time - start_time, deaths_count)
    except Exception as e:
        log.exception("[❌] Fail processing message: deaths %d", deaths_count)
        # Остановиться после 3 попыток
        if deaths_count < 2:
            log.exception("[⭕] Try retry process message:")
            # Возвращаем сообщение обратно в очередь для повторной обработки
            # Без вызова nack потребитель просто остановит работу
            # Нам придется явно перезапускать процесс
            await msg.nack(requeue=False)
            # await msg.nack(requeue=False) === msg.reject() - сообщение не вернется в очередь
            # await msg.nack(multiple=True) - вернет в очередь {prefetch_count} сообщений при ошибке обработки любого из них
            return
        log.warning("[🕒] Delay task %r after %d retries for retro", msg.body, deaths_count)
        # Отправляем в логи задачу, которую не получилось обработать
        await msg.channel.basic_publish(
            body=msg.body,
            properties=msg.properties,
            exchange=MQ_NOTIFICATIONS_EXCHANGE,
            routing_key=MQ_FAILED_NOTIFICATIONS_QUEUE
        )
        await msg.ack()


async def main():
    async with DirectNotifyRabbit(durable=True, retry=True) as rabbit:
        log.info("Created channel %s", rabbit.channel)
        await rabbit.consume_messages(
            process_message_callback=process_message,
            prefetch_count=1,
            with_dlq=True
        )
        await asyncio.Event().wait()


if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Bye!")
