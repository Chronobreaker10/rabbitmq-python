import logging

from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

import aio_pika
from aio_pika.abc import AbstractRobustConnection

# DEFAULT_LOG_FORMAT = "[%(asctime)s.%(msecs)03d] %(funcName)20s %(module)s:%(lineno)d %(levelname)-8s - %(message)s"
DEFAULT_LOG_FORMAT = "%(name)s %(module)s:%(lineno)d %(levelname)-6s - %(message)s"

RMQ_HOST = "0.0.0.0"
RMQ_PORT = 5672
RMQ_USER = "user"
RMQ_PASSWORD = "password"
RMQ_TTL_MS = 60_000
RMQ_DLQ_TTL_MS = 120_000

MQ_NOTIFICATIONS_EXCHANGE = "notifications"
MQ_NOTIFICATIONS_QUEUE = "notifications"
MQ_NOTIFICATIONS_DEAD_LETTER_QUEUE = "dlq-notifications"
MQ_NOTIFICATIONS_DEAD_LETTER_EXCHANGE = "dlx-notifications"

MQ_EMAIL_UPDATES_EXCHANGE = "email.updates"
MQ_EMAIL_UPDATES_EMAIL_QUEUE = "email.updates.email_queue"
MQ_EMAIL_UPDATES_PUSH_QUEUE = "email.updates.push_queue"

@asynccontextmanager
async def get_connection() -> AsyncGenerator[AbstractRobustConnection, Any]:
    yield await aio_pika.connect_robust(
        f"amqp://{RMQ_USER}:{RMQ_PASSWORD}@localhost:{RMQ_PORT}/",
    )


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
        format=DEFAULT_LOG_FORMAT
    )
    logging.getLogger("aio_pika").setLevel(logging.WARNING)
