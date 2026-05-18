from __future__ import annotations

import logging
from typing import Callable, Coroutine, Any

import aio_pika
from aio_pika import Message
from aio_pika.abc import AbstractRobustChannel, AbstractIncomingMessage, AbstractRobustExchange, AbstractQueue

from rabbit import RabbitBase, config
from rabbit.base import RabbitConfig

log = logging.getLogger(__name__)


class DirectNotifyRabbit(RabbitBase):
    _channel: AbstractRobustChannel
    _exchange: AbstractRobustExchange

    def __init__(self, settings: RabbitConfig = RabbitConfig(), durable: bool = False) -> None:
        super().__init__(settings)
        # durable - обменник не исчезнет после остановки Rabbit
        self._durable = durable

    @property
    def exchange(self) -> AbstractRobustExchange:
        return self._exchange

    async def declare_direct_notify_exchange(self) -> AbstractRobustExchange:
        self._exchange = await self._channel.declare_exchange(
            config.MQ_NOTIFICATIONS_EXCHANGE,
            aio_pika.ExchangeType.DIRECT,
            durable=self._durable
        )
        return self._exchange

    async def declare_direct_notify_dead_letter_exchange(self) -> None:
        await self._channel.declare_exchange(
            config.MQ_NOTIFICATIONS_DEAD_LETTER_EXCHANGE,
            aio_pika.ExchangeType.DIRECT,
            durable=self._durable
        )

    async def declare_direct_notify_queue(
            self,
            with_dlq: bool = False,
            ttl: int | None = None
    ) -> AbstractQueue:
        await self.declare_direct_notify_exchange()
        if with_dlq:
            await self.declare_direct_notify_dead_letter_exchange()
            # Параметр durable указывает не удалять очередь после остановки Rabbit
            dlq = await self._channel.declare_queue(
                name=config.MQ_NOTIFICATIONS_DEAD_LETTER_QUEUE,
                arguments={
                    "x-message-ttl": config.RMQ_DLQ_TTL_MS
                },
                durable=self._durable
            )
            await dlq.bind(
                exchange=config.MQ_NOTIFICATIONS_DEAD_LETTER_EXCHANGE,
                routing_key=config.MQ_NOTIFICATIONS_DEAD_LETTER_QUEUE
            )
        arguments = {}
        if with_dlq:
            arguments["x-dead-letter-exchange"] = config.MQ_NOTIFICATIONS_DEAD_LETTER_EXCHANGE
            arguments["x-dead-letter-routing-key"] = config.MQ_NOTIFICATIONS_DEAD_LETTER_QUEUE
        if ttl is not None:
            arguments["x-message-ttl"] = ttl
        queue = await self._channel.declare_queue(
            name=config.MQ_NOTIFICATIONS_QUEUE,
            arguments=arguments,
            durable=self._durable
        )
        await queue.bind(
            exchange=config.MQ_NOTIFICATIONS_EXCHANGE,
            routing_key=config.MQ_NOTIFICATIONS_QUEUE
        )
        return queue

    async def publish_message(self, body: str) -> None:
        log.debug("Send message to RabbitMQ %s", body)
        await self.exchange.publish(
            Message(
                body=body.encode('utf-8'),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT if self._durable else aio_pika.DeliveryMode.NOT_PERSISTENT
            ),
            routing_key=config.MQ_NOTIFICATIONS_QUEUE,
        )
        log.warning("Published message to RabbitMQ %s", body)

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
