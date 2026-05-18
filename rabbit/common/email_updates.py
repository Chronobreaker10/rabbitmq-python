import logging
from typing import Callable, Coroutine, Any

import aio_pika
from aio_pika.abc import AbstractRobustChannel, AbstractIncomingMessage, AbstractRobustExchange

from rabbit import RabbitBase, config

log = logging.getLogger(__name__)


class EmailUpdatesRabbit(RabbitBase):
    _channel: AbstractRobustChannel
    _exchange: AbstractRobustExchange

    @property
    def exchange(self) -> AbstractRobustExchange:
        return self._exchange

    async def declare_email_updates_exchange(self) -> AbstractRobustExchange:
        self._exchange = await self._channel.declare_exchange(config.MQ_EMAIL_UPDATES_EXCHANGE,
                                                              aio_pika.ExchangeType.FANOUT)
        return self._exchange

    async def declare_email_updates_queue(
            self,
            queue_name: str = "",
            exclusive: bool = True,
    ):
        """Создаем эксклюзивную очередь без имени для рассылки
        (Она будет существовать только в рамках подключения к каналу и иметь уникальное имя)"""
        await self.declare_email_updates_exchange()
        queue = await self._channel.declare_queue(name=queue_name, exclusive=exclusive)
        await queue.bind(exchange=config.MQ_EMAIL_UPDATES_EXCHANGE)
        return queue

    async def consume_messages(
            self,
            process_message_callback: Callable[
                [AbstractIncomingMessage], Coroutine[AbstractIncomingMessage, Any, None]],
            prefetch_count: int = 1,
            queue_name: str = "",
    ):
        await self._channel.set_qos(prefetch_count=prefetch_count)
        # Очередь с именем будет постоянно существовать и получит
        # сообщения после перезапуска в отличие от эксклюзивной
        queue = await self.declare_email_updates_queue(queue_name, exclusive=not queue_name)
        await queue.consume(process_message_callback, no_ack=False)
        log.warning(f"Queue {queue.name} waiting for messages...")
