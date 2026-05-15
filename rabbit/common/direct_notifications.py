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
        self._exchange = await self._channel.declare_exchange(config.MQ_NOTIFICATIONS_EXCHANGE_NAME,
                                                              aio_pika.ExchangeType.DIRECT)
        return self._exchange

    async def declare_direct_notify_queue(self):
        await self.declare_direct_notify_exchange()
        queue = await self._channel.declare_queue(name=config.MQ_NOTIFICATIONS_QUEUE_NAME)
        await queue.bind(
            exchange=config.MQ_NOTIFICATIONS_EXCHANGE_NAME,
            routing_key=config.MQ_NOTIFICATIONS_ROUTING_KEY
        )
        return queue

    async def consume_messages(
            self,
            process_message_callback: Callable[
                [AbstractIncomingMessage], Coroutine[AbstractIncomingMessage, Any, None]],
            prefetch_count: int = 1
    ):
        await self._channel.set_qos(prefetch_count=prefetch_count)
        queue = await self.declare_direct_notify_queue()
        await queue.consume(process_message_callback, no_ack=False)
        log.warning(f"Queue {queue.name} waiting for messages...")
