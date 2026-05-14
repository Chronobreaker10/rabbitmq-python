import asyncio
import logging
import time

from aio_pika.abc import AbstractRobustChannel, AbstractIncomingMessage

from config import configure_logging, get_connection, MQ_ROUTING_KEY

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
        await msg.ack()
        log.info("[X] Finish processing message %r in %.2fs", msg.body, end_time - start_time)
    except Exception as e:
        log.exception("Error processing message: %s", e)


async def consume_messages(channel: AbstractRobustChannel) -> None:
    # По умолчанию RabbitMQ использует RoundRobin (равномерное распределение по кругу)
    # Все потребители получат сразу все сообщения в одинаковом количестве
    # Если хотим равномерно распределить нагрузку надо указать загружаемое количество сообщений

    # Указываем загрузить только одно сообщение, а следующее только после обработки предыдущего
    await channel.set_qos(prefetch_count=1)

    queue = await channel.get_queue(MQ_ROUTING_KEY)
    await queue.consume(process_message, no_ack=True)
    # Альтернативный способ через цикл
    # async with queue.iterator() as queue_iter:
    #     async for message in queue_iter:
    #         await process_message(message)


async def main():
    async with get_connection() as conn:
        log.info("Connecting to RabbitMQ %s", conn)
        async with conn.channel() as channel:
            log.info("Created channel %s", conn)
            await consume_messages(channel)
            await asyncio.Event().wait()


if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Bye!")
