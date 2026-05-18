import logging
from typing import Callable, Coroutine, Any

import aio_pika
from aio_pika.abc import AbstractRobustChannel, AbstractIncomingMessage, AbstractRobustExchange

from rabbit import RabbitBase, config

log = logging.getLogger(__name__)


class DirectNotifyRabbit(RabbitBase):
    _channel: AbstractRobustChannel
    _exchange: AbstractRobustExchange

    @property
    def exchange(self) -> AbstractRobustExchange:
        return self._exchange

    async def declare_direct_notify_exchange(self) -> AbstractRobustExchange:
        self._exchange = await self._channel.declare_exchange(config.MQ_NOTIFICATIONS_EXCHANGE,
                                                              aio_pika.ExchangeType.DIRECT)
        return self._exchange

    async def declare_direct_notify_dead_letter_exchange(self) -> None:
        await self._channel.declare_exchange(config.MQ_NOTIFICATIONS_DEAD_LETTER_EXCHANGE, aio_pika.ExchangeType.DIRECT)

    async def declare_direct_notify_queue(self):
        await self.declare_direct_notify_exchange()
        await self.declare_direct_notify_dead_letter_exchange()
        # Параметр durable указывает не удалять очередь после остановки Rabbit
        dlq = await self._channel.declare_queue(name=config.MQ_NOTIFICATIONS_DEAD_LETTER_QUEUE)
        await dlq.bind(
            exchange=config.MQ_NOTIFICATIONS_DEAD_LETTER_EXCHANGE,
            routing_key=config.MQ_NOTIFICATIONS_DEAD_LETTER_QUEUE
        )
        queue = await self._channel.declare_queue(
            name=config.MQ_NOTIFICATIONS_QUEUE,
            arguments={
                "x-dead-letter-exchange": config.MQ_NOTIFICATIONS_DEAD_LETTER_EXCHANGE,
                "x-dead-letter-routing-key": config.MQ_NOTIFICATIONS_DEAD_LETTER_QUEUE
            }
        )
        await queue.bind(
            exchange=config.MQ_NOTIFICATIONS_EXCHANGE,
            routing_key=config.MQ_NOTIFICATIONS_QUEUE
        )
        return queue

    async def consume_messages(
            self,
            process_message_callback: Callable[
                [AbstractIncomingMessage], Coroutine[AbstractIncomingMessage, Any, None]],
            prefetch_count: int = 1
    ):
        # По умолчанию RabbitMQ использует RoundRobin (равномерное распределение по кругу)
        # Все потребители получат сразу все сообщения в одинаковом количестве
        # Если хотим равномерно распределить нагрузку надо указать загружаемое количество сообщений

        # Указываем загрузить только одно сообщение, а следующее только после обработки предыдущего
        await self._channel.set_qos(prefetch_count=prefetch_count)
        queue = await self.declare_direct_notify_queue()
        await queue.consume(process_message_callback, no_ack=False)

        # Альтернативный способ через цикл
        # async with queue.iterator() as queue_iter:
        #     async for message in queue_iter:
        #         await process_message(message)
        log.warning(f"Queue {queue.name} waiting for messages...")
