import logging

from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

import aio_pika
from aio_pika.abc import AbstractRobustConnection

RMQ_HOST = "0.0.0.0"
RMQ_PORT = 5672
RMQ_USER = "user"
RMQ_PASSWORD = "password"

MQ_EXCHANGE = "notifications"
MQ_ROUTING_KEY = "notifications"

MQ_EMAIL_UPDATES_EXCHANGE_NAME = "email.updates"
MQ_EMAIL_UPDATES_EMAIL_QUEUE_NAME = "email.updates.email_queue"
MQ_EMAIL_UPDATES_PUSH_QUEUE_NAME = "email.updates.push_queue"


@asynccontextmanager
async def get_connection() -> AsyncGenerator[AbstractRobustConnection, Any]:
    yield await aio_pika.connect_robust(
        f"amqp://{RMQ_USER}:{RMQ_PASSWORD}@localhost:{RMQ_PORT}/",
    )


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s.%(msecs)03d] %(funcName)20s %(module)s:%(lineno)d %(levelname)-8s - %(message)s"
    )
