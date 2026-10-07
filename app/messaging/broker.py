import asyncio
import logging

from faststream.rabbit import Channel, RabbitBroker

from app.config import Settings
from app.messaging.retry import RetryPolicy

log = logging.getLogger(__name__)


def build_retry_policy(settings: Settings) -> RetryPolicy:
    return RetryPolicy(
        max_attempts=settings.max_attempts,
        base_delay=settings.retry_base_delay,
        backoff_factor=settings.retry_backoff_factor,
    )


def build_broker(settings: Settings, *, prefetch_count: int | None = None) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(
            prefetch_count=prefetch_count,
            publisher_confirms=True,  # publish() returns only after the broker confirms
            on_return_raises=True,  # unroutable (mandatory) messages raise instead of being dropped
        ),
    )


async def connect_with_retry(broker: RabbitBroker, *, attempts: int = 15, delay: float = 2.0) -> None:
    """Connect to RabbitMQ, tolerating a broker that is still booting (docker compose start-up)."""
    for attempt in range(1, attempts + 1):
        try:
            await broker.connect()
            return
        except Exception as exc:
            if attempt == attempts:
                raise
            log.warning(
                "RabbitMQ not ready (%s: %s); retry %d/%d in %.0fs",
                type(exc).__name__,
                exc,
                attempt,
                attempts,
                delay,
            )
            await asyncio.sleep(delay)
