"""FastStream consumer: the single handler that does everything for ``payments.new``.

Run with ``faststream run app.consumer.app:app``.
"""

import logging

import httpx
from faststream import FastStream
from faststream.exceptions import AckMessage, RejectMessage
from faststream.rabbit import RabbitMessage

from app.config import get_settings
from app.consumer.gateway import EmulatedGateway
from app.consumer.processor import NonRetryableError, PaymentProcessor
from app.consumer.repository import SqlPaymentRepository
from app.consumer.webhook import HttpWebhookSender
from app.db import dispose_engine, get_session_factory
from app.logging import configure_logging
from app.messaging.broker import build_broker, build_retry_policy
from app.messaging.events import PaymentCreatedEvent
from app.messaging.topology import (
    ATTEMPT_HEADER,
    LAST_ERROR_HEADER,
    declare_topology,
    payments_exchange,
    payments_new_queue,
    retry_exchange,
    retry_routing_key,
)

log = logging.getLogger(__name__)

settings = get_settings()
retry_policy = build_retry_policy(settings)
broker = build_broker(settings, prefetch_count=settings.consumer_prefetch)
app = FastStream(broker)


class _State:
    http: httpx.AsyncClient
    processor: PaymentProcessor


state = _State()


@app.on_startup
async def on_startup() -> None:
    configure_logging(settings.log_level)
    state.http = httpx.AsyncClient(timeout=settings.webhook_timeout, follow_redirects=False)
    state.processor = PaymentProcessor(
        repository=SqlPaymentRepository(get_session_factory()),
        gateway=EmulatedGateway(
            min_delay=settings.gateway_min_delay,
            max_delay=settings.gateway_max_delay,
            success_rate=settings.gateway_success_rate,
        ),
        webhooks=HttpWebhookSender(state.http, secret=settings.webhook_secret),
    )


@app.after_startup
async def on_after_startup() -> None:
    await declare_topology(broker, retry_policy)
    log.info(
        "Consumer ready: max_attempts=%d, delays=%s",
        retry_policy.max_attempts,
        [retry_policy.delay_for(a) for a in retry_policy.retry_attempts],
    )


@app.on_shutdown
async def on_shutdown() -> None:
    await state.http.aclose()
    await dispose_engine()


@broker.subscriber(payments_new_queue, payments_exchange)
async def handle_new_payment(event: PaymentCreatedEvent, msg: RabbitMessage) -> None:
    attempt = int(msg.headers.get(ATTEMPT_HEADER, 1))
    log.info("Processing payment %s (attempt %d/%d)", event.payment_id, attempt, retry_policy.max_attempts)

    try:
        await state.processor.process(event.payment_id)
    except NonRetryableError as exc:
        log.error("Payment %s rejected permanently: %s", event.payment_id, exc)
        raise RejectMessage() from None  # reject without requeue -> DLX -> payments.dlq
    except Exception as exc:
        await _handle_failure(event, msg, attempt, exc)


async def _handle_failure(
    event: PaymentCreatedEvent, msg: RabbitMessage, attempt: int, exc: Exception
) -> None:
    error = f"{type(exc).__name__}: {exc}"
    if not retry_policy.should_retry(attempt):
        log.error(
            "Payment %s failed on final attempt %d, sending to DLQ: %s", event.payment_id, attempt, error
        )
        raise RejectMessage() from None

    delay = retry_policy.delay_for(attempt)
    log.warning("Payment %s attempt %d failed (%s); retry in %.1fs", event.payment_id, attempt, error, delay)
    # Publish a copy into the wait-queue for this attempt; its TTL routes it back to
    # payments.new. Only after the copy is confirmed do we ack the original.
    await broker.publish(
        event,
        exchange=retry_exchange,
        routing_key=retry_routing_key(attempt),
        message_id=msg.message_id,
        correlation_id=msg.correlation_id,
        headers={ATTEMPT_HEADER: attempt + 1, LAST_ERROR_HEADER: error[:200]},
        persist=True,
    )
    raise AckMessage() from None
