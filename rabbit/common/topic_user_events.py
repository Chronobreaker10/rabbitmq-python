import logging
from typing import Callable, Coroutine, Any

import aio_pika
from aio_pika.abc import AbstractRobustChannel, AbstractIncomingMessage, AbstractRobustExchange

from rabbit import RabbitBase, config

log = logging.getLogger(__name__)


class TopicUserEventsRabbit(RabbitBase):
    _channel: AbstractRobustChannel
    _exchange: AbstractRobustExchange

    @property
    def exchange(self) -> AbstractRobustExchange:
        return self._exchange

    async def declare_user_events_exchange(self) -> AbstractRobustExchange:
        self._exchange = await self._channel.declare_exchange(config.MQ_USER_EVENTS_EXCHANGE,
                                                              aio_pika.ExchangeType.TOPIC)
        return self._exchange

    async def declare_user_registered_queue(self):
        await self.declare_user_events_exchange()
        queue = await self._channel.declare_queue(name=config.MQ_USER_EVENTS_REGISTERED_QUEUE)
        await queue.bind(
            exchange=config.MQ_USER_EVENTS_EXCHANGE,
            routing_key=config.MQ_USER_EVENTS_REGISTER_ROUTING_KEY
        )
        return queue

    async def declare_user_verified_queue(self):
        await self.declare_user_events_exchange()
        queue = await self._channel.declare_queue(name=config.MQ_USER_EVENTS_VERIFIED_QUEUE)
        await queue.bind(
            exchange=config.MQ_USER_EVENTS_EXCHANGE,
            routing_key=config.MQ_USER_EVENTS_VERIFIED_ROUTING_KEY
        )
        return queue

    async def consume_messages(
            self,
            process_message_callback: Callable[
                [AbstractIncomingMessage], Coroutine[AbstractIncomingMessage, Any, None]],
            queue_name: str,
            prefetch_count: int = 1,
    ):
        await self._channel.set_qos(prefetch_count=prefetch_count)
        await self.declare_user_verified_queue()
        await self.declare_user_registered_queue()
        queue = await self._channel.get_queue(queue_name)
        await queue.consume(process_message_callback, no_ack=False, timeout=5)
        # Или await queue.get() в цикле
        log.warning(f"Queue {queue.name} waiting for messages...")
