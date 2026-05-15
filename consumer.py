import asyncio
import logging
import time
from random import random

from aio_pika.abc import AbstractRobustChannel, AbstractIncomingMessage

from rabbit.config import configure_logging, MQ_NOTIFICATIONS_ROUTING_KEY
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
            raise Exception("Internal error")
        # Подтверждаем обработку сообщения (оно исчезнет из очереди)
        await msg.ack()

        log.info("[X] Finish processing message %r in %.2fs", msg.body, end_time - start_time)
    except Exception as e:
        log.exception("Error processing message: %s", e)
        log.exception("Try retry process message:")
        # Возвращаем сообщение обратно в очередь для повторной обработки
        # Без вызова nack потребитель просто остановит работу
        # Нам придется явно перезапускать процесс
        await msg.nack()
        # await msg.nack(requeue=False) === msg.reject() - сообщение не вернется в очередь
        # await msg.nack(multiple=True) - вернет в очередь {prefetch_count} сообщений при ошибке обработки любого из них


async def consume_messages(channel: AbstractRobustChannel) -> None:
    # По умолчанию RabbitMQ использует RoundRobin (равномерное распределение по кругу)
    # Все потребители получат сразу все сообщения в одинаковом количестве
    # Если хотим равномерно распределить нагрузку надо указать загружаемое количество сообщений

    # Указываем загрузить только одно сообщение, а следующее только после обработки предыдущего
    await channel.set_qos(prefetch_count=3)
    # Параметр durable указывает не удалять очередь после остановки Rabbit
    queue = await channel.declare_queue(MQ_NOTIFICATIONS_ROUTING_KEY, durable=True)
    # queue = await channel.get_queue(MQ_ROUTING_KEY)
    await queue.consume(process_message, no_ack=False)
    # Альтернативный способ через цикл
    # async with queue.iterator() as queue_iter:
    #     async for message in queue_iter:
    #         await process_message(message)


async def main():
    async with RabbitBase() as rabbit:
        log.info("Created channel %s", rabbit.channel)
        await consume_messages(rabbit.channel)
        await asyncio.Event().wait()


if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Bye!")
