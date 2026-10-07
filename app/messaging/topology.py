"""RabbitMQ topology shared by the outbox relay and the consumer.

::

    outbox relay ──► [payments] ──payments.new──► (payments.new) ──► consumer
                                                       │  reject (no attempts left / non-retryable)
                                                       ▼
                                                 [payments.dlx] ──payments.dead──► (payments.dlq)

    consumer, retryable failure on attempt N < max:
        publish copy ──► [payments.retry] ──payments.retry.N──► (payments.retry.N, TTL = base*factor^(N-1))
                                                                      │ message expires → dead-lettered
                                                                      ▼
                                                               [payments] ──payments.new──► (payments.new)

One wait-queue per attempt (instead of a single queue with per-message TTL) avoids
head-of-line blocking: RabbitMQ only expires messages from the head of a queue.
"""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.messaging.retry import RetryPolicy

PAYMENTS_EXCHANGE = "payments"
RETRY_EXCHANGE = "payments.retry"
DLX_EXCHANGE = "payments.dlx"

PAYMENTS_NEW_QUEUE = "payments.new"
PAYMENTS_NEW_ROUTING_KEY = "payments.new"
DLQ_QUEUE = "payments.dlq"
DLQ_ROUTING_KEY = "payments.dead"

ATTEMPT_HEADER = "x-attempt"
LAST_ERROR_HEADER = "x-last-error"

payments_exchange = RabbitExchange(PAYMENTS_EXCHANGE, type=ExchangeType.DIRECT, durable=True)
retry_exchange = RabbitExchange(RETRY_EXCHANGE, type=ExchangeType.DIRECT, durable=True)
dlx_exchange = RabbitExchange(DLX_EXCHANGE, type=ExchangeType.DIRECT, durable=True)

payments_new_queue = RabbitQueue(
    PAYMENTS_NEW_QUEUE,
    durable=True,
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DLX_EXCHANGE,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    },
)

dlq_queue = RabbitQueue(DLQ_QUEUE, durable=True, routing_key=DLQ_ROUTING_KEY)


def retry_routing_key(attempt: int) -> str:
    return f"payments.retry.{attempt}"


def retry_queue(attempt: int, delay_seconds: float) -> RabbitQueue:
    """Wait-queue for messages that failed on ``attempt``; expires them back into payments.new."""
    return RabbitQueue(
        retry_routing_key(attempt),
        durable=True,
        routing_key=retry_routing_key(attempt),
        arguments={
            "x-message-ttl": int(delay_seconds * 1000),
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE,
            "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
        },
    )


async def declare_topology(broker: RabbitBroker, policy: RetryPolicy) -> None:
    """Idempotently declare all exchanges/queues and bindings. Requires a connected broker."""

    async def declare_and_bind(queue: RabbitQueue, exchange: RabbitExchange) -> None:
        ex = await broker.declare_exchange(exchange)
        q = await broker.declare_queue(queue)
        await q.bind(ex, routing_key=queue.routing_key)

    await declare_and_bind(dlq_queue, dlx_exchange)  # DLX must exist before queues referencing it
    await declare_and_bind(payments_new_queue, payments_exchange)
    for attempt in policy.retry_attempts:
        await declare_and_bind(retry_queue(attempt, policy.delay_for(attempt)), retry_exchange)
