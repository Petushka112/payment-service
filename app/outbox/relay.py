"""Outbox relay: moves events from the ``outbox`` table to RabbitMQ.

Guarantees at-least-once publication: a row is marked ``published_at`` only after the
broker confirmed the message. If the process dies in between, the row is picked up
again and the event is published twice; consumers are idempotent, so that is safe.

``FOR UPDATE SKIP LOCKED`` lets several relay instances run side by side without
publishing the same row concurrently.
"""

import asyncio
import contextlib
import logging
import signal
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker, RabbitExchange
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.db import dispose_engine, get_session_factory
from app.logging import configure_logging
from app.messaging.broker import build_broker, build_retry_policy, connect_with_retry
from app.messaging.topology import ATTEMPT_HEADER, declare_topology, payments_exchange
from app.models import OutboxEvent

log = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        exchange: RabbitExchange = payments_exchange,
        *,
        batch_size: int = 100,
        poll_interval: float = 0.5,
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._exchange = exchange
        self._batch_size = batch_size
        self._poll_interval = poll_interval

    async def publish_pending(self) -> int:
        """Publish one batch of unpublished events. Returns the number published."""
        async with self._session_factory() as session:
            stmt = (
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.created_at)
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            events = (await session.execute(stmt)).scalars().all()

            published = 0
            for event in events:
                event.publish_attempts += 1
                try:
                    await self._broker.publish(
                        event.payload,
                        exchange=self._exchange,
                        routing_key=event.event_type,
                        message_id=str(event.id),
                        headers={ATTEMPT_HEADER: 1, "x-event-type": event.event_type},
                        persist=True,
                    )
                except Exception as exc:  # broker down, unroutable, confirm timeout...
                    event.last_error = f"{type(exc).__name__}: {exc}"[:1000]
                    log.warning("Failed to publish outbox event %s: %s", event.id, event.last_error)
                    break  # keep ordering; the rest of the batch stays unpublished
                event.published_at = datetime.now(UTC)
                event.last_error = None
                published += 1
                log.info("Published %s for %s %s", event.event_type, event.aggregate_type, event.aggregate_id)

            await session.commit()
            return published

    async def run_forever(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                published = await self.publish_pending()
            except Exception:
                log.exception("Outbox relay iteration failed")
                published = 0
            if published == 0:
                # Idle: wait for the poll interval (or an earlier stop signal).
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=self._poll_interval)


def _install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            signal.signal(sig, lambda *_: stop.set())


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    broker = build_broker(settings)
    await connect_with_retry(broker)
    await declare_topology(broker, build_retry_policy(settings))

    relay = OutboxRelay(
        get_session_factory(),
        broker,
        batch_size=settings.outbox_batch_size,
        poll_interval=settings.outbox_poll_interval,
    )
    stop = asyncio.Event()
    _install_signal_handlers(stop)
    log.info(
        "Outbox relay started (poll every %.2fs, batch %d)",
        settings.outbox_poll_interval,
        settings.outbox_batch_size,
    )
    try:
        await relay.run_forever(stop)
    finally:
        await broker.stop()
        await dispose_engine()
        log.info("Outbox relay stopped")


if __name__ == "__main__":
    asyncio.run(main())
