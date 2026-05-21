from __future__ import annotations

from dataclasses import dataclass

import aio_pika
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel


@dataclass
class RabbitConfig:
    host: str = "localhost"
    port: int = 5672
    user: str = "user"
    password: str = "password"


class RabbitException(Exception):
    pass


class RabbitBase:
    def __init__(self, config: RabbitConfig = RabbitConfig(), publish_confirms: bool = False) -> None:
        self.config: RabbitConfig = config
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractRobustChannel | None = None
        self._publish_confirms = publish_confirms

    @property
    def url(self):
        return f"amqp://{self.config.user}:{self.config.password}@{self.config.host}:{self.config.port}/"

    @property
    def connection(self) -> AbstractRobustConnection:
        if not self._connection or self._connection.is_closed:
            raise RabbitException
        return self._connection

    @property
    def channel(self) -> AbstractRobustChannel:
        if not self._channel or self._channel.is_closed:
            raise RabbitException
        return self._channel

    async def __aenter__(self) -> RabbitBase:
        self._connection = await aio_pika.connect_robust(self.url)
        if self._connection:
            self._channel = await self._connection.channel()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self._channel or not self._connection:
            raise RabbitException
        if not self._channel.is_closed:
            await self._channel.close()
        if not self._connection.is_closed:
            await self._connection.close()
