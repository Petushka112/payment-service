"""Persistence operations used by the consumer, kept behind a small interface so the
processing logic can be unit-tested with an in-memory fake."""

import uuid
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import GatewayResult
from app.enums import PaymentStatus
from app.models import Payment


class PaymentRepository(Protocol):
    async def get(self, payment_id: uuid.UUID) -> Payment | None: ...

    async def finalize(self, payment_id: uuid.UUID, result: GatewayResult) -> Payment:
        """Move ``pending`` -> final status (compare-and-set). Returns the stored row,
        which may hold an earlier outcome if another worker finished first."""
        ...

    async def mark_webhook_delivered(self, payment_id: uuid.UUID) -> None: ...

    async def record_error(self, payment_id: uuid.UUID, error: str) -> None: ...


class SqlPaymentRepository:
    """Each method is its own short transaction: no locks are held while the
    (slow) gateway or webhook call is in flight."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        async with self._session_factory() as session:
            return await session.get(Payment, payment_id)

    async def finalize(self, payment_id: uuid.UUID, result: GatewayResult) -> Payment:
        async with self._session_factory() as session:
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
                .values(
                    status=result.status,
                    failure_reason=result.failure_reason,
                    processed_at=datetime.now(UTC),
                    last_error=None,
                )
            )
            await session.commit()
            return (await session.execute(select(Payment).where(Payment.id == payment_id))).scalar_one()

    async def mark_webhook_delivered(self, payment_id: uuid.UUID) -> None:
        async with self._session_factory() as session:
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id)
                .values(webhook_delivered_at=datetime.now(UTC), last_error=None)
            )
            await session.commit()

    async def record_error(self, payment_id: uuid.UUID, error: str) -> None:
        async with self._session_factory() as session:
            await session.execute(
                update(Payment).where(Payment.id == payment_id).values(last_error=error[:1000])
            )
            await session.commit()
