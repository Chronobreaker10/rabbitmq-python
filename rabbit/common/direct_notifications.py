from __future__ import annotations

import asyncio
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

    def __init__(
            self,
            settings: RabbitConfig = RabbitConfig(),
            durable: bool = False,
            retry: bool = False,
    ) -> None:
        super().__init__(settings)
        # durable - обменник не исчезнет после остановки Rabbit
        self._durable = durable
        self._retry = retry

    @property
    def exchange(self) -> AbstractRobustExchange:
        return self._exchange

    async def clear(self):
        await self._channel.queue_delete(config.MQ_NOTIFICATIONS_DEAD_LETTER_QUEUE)
        await self._channel.queue_delete(config.MQ_NOTIFICATIONS_QUEUE)
        await self._channel.exchange_delete(config.MQ_FAILED_NOTIFICATIONS_QUEUE)
        await self._channel.exchange_delete(config.MQ_NOTIFICATIONS_DEAD_LETTER_EXCHANGE)
        await self._channel.exchange_delete(config.MQ_NOTIFICATIONS_EXCHANGE)

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

    async def declare_failed_queue(self):
        failed_queue = await self._channel.declare_queue(
            name=config.MQ_FAILED_NOTIFICATIONS_QUEUE,
            durable=self._durable
        )
        await failed_queue.bind(
            exchange=config.MQ_NOTIFICATIONS_EXCHANGE,
            routing_key=config.MQ_FAILED_NOTIFICATIONS_QUEUE
        )

    async def declare_ddl_queue(self):
        await self.declare_direct_notify_dead_letter_exchange()
        arguments = {}
        if self._retry:
            arguments["x-dead-letter-exchange"] = config.MQ_NOTIFICATIONS_EXCHANGE
            arguments["x-dead-letter-routing-key"] = config.MQ_NOTIFICATIONS_QUEUE
            arguments["x-message-ttl"] = config.MQ_FAILED_NOTIFICATIONS_RETRY_SECS * 1000
        else:
            arguments["x-message-ttl"] = config.RMQ_DLQ_TTL_MS
        # Параметр durable указывает не удалять очередь после остановки Rabbit
        dlq = await self._channel.declare_queue(
            name=config.MQ_NOTIFICATIONS_DEAD_LETTER_QUEUE,
            arguments=arguments,
            durable=self._durable
        )
        await dlq.bind(
            exchange=config.MQ_NOTIFICATIONS_DEAD_LETTER_EXCHANGE,
            routing_key=config.MQ_NOTIFICATIONS_DEAD_LETTER_QUEUE
        )

    async def declare_direct_notify_queue(
            self,
            with_dlq: bool = False,
            ttl: int | None = None
    ) -> AbstractQueue:
        await self.declare_direct_notify_exchange()
        if with_dlq:
            await self.declare_ddl_queue()
        if self._retry:
            await self.declare_failed_queue()
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
        try:
            log.debug("Send message to RabbitMQ %s", body)
            t = asyncio.create_task(self.exchange.publish(
                Message(
                    body=body.encode('utf-8'),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT if self._durable else aio_pika.DeliveryMode.NOT_PERSISTENT,
                ),
                routing_key=config.MQ_NOTIFICATIONS_QUEUE,
                # Гарантия доставки (True по умолчанию)
                # выбрасываем исключение если не получилось опубликовать сообщения
                mandatory=True,
                timeout=5.0
            ))
            log.warning("Publishing... message to RabbitMQ %s", body)
            await asyncio.sleep(0.05)
            await self._channel.close()
            await t
            await asyncio.sleep(6)
            await self._channel.reopen()
            await t
            log.warning("Published message to RabbitMQ %s", body)
        except asyncio.TimeoutError:
            print("Ошибка: Не удалось дождаться подтверждения от брокера в течение 5 секунд.")
            # Здесь нужно реализовать логику повторной отправки или сохранения в локальное хранилище
        except aio_pika.exceptions.DeliveryError as e:
            # Эта ошибка возникает, например, если сообщение с флагом mandatory=true не может быть маршрутизировано
            print(f"Ошибка доставки: {e}")

    async def consume_messages(
            self,
            process_message_callback: Callable[
                [AbstractIncomingMessage], Coroutine[AbstractIncomingMessage, Any, None]],
            prefetch_count: int = 1,
            with_dlq: bool = False
    ):
        # По умолчанию RabbitMQ использует RoundRobin (равномерное распределение по кругу)
        # Все потребители получат сразу все сообщения в одинаковом количестве
        # Если хотим равномерно распределить нагрузку надо указать загружаемое количество сообщений

        # Указываем загрузить только одно сообщение, а следующее только после обработки предыдущего
        await self._channel.set_qos(prefetch_count=prefetch_count)
        queue = await self.declare_direct_notify_queue(with_dlq=with_dlq)
        await queue.consume(process_message_callback, no_ack=False)

        # Альтернативный способ через цикл
        # async with queue.iterator() as queue_iter:
        #     async for message in queue_iter:
        #         await process_message(message)
        log.warning(f"Queue {queue.name} waiting for messages...")
