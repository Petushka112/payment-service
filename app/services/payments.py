"""Payment use-cases: idempotent creation (with outbox event) and lookup."""

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging.topology import PAYMENTS_NEW_ROUTING_KEY
from app.models import OutboxEvent, Payment
from app.schemas import PaymentCreate


class IdempotencyConflictError(Exception):
    """Same Idempotency-Key was already used with a different request body."""


@dataclass(slots=True)
class CreateResult:
    payment: Payment
    replayed: bool  # True when an existing payment was returned for a repeated key


def request_fingerprint(data: PaymentCreate) -> str:
    """Stable hash of the request body, independent of key order / whitespace."""
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, data: PaymentCreate, idempotency_key: str) -> CreateResult:
        """Create a payment and its outbox event atomically.

        Idempotency: the unique constraint on ``payments.idempotency_key`` is the
        source of truth, so concurrent requests with the same key cannot both insert.
        The loser of the race gets an IntegrityError and returns the existing row
        (or a conflict if the body differs).
        """
        fingerprint = request_fingerprint(data)
        # Generate the id here (not via the column default, which only fires at flush time)
        # so the outbox payload can reference it inside the same transaction.
        payment_id = uuid.uuid4()

        payment = Payment(
            id=payment_id,
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata_=data.metadata,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
            webhook_url=str(data.webhook_url),
        )
        event = OutboxEvent(
            aggregate_type="payment",
            aggregate_id=payment_id,
            event_type=PAYMENTS_NEW_ROUTING_KEY,
            payload={
                "payment_id": str(payment_id),
                "occurred_at": datetime.now(UTC).isoformat(),
            },
        )

        self._session.add_all([payment, event])
        try:
            await self._session.commit()
        except IntegrityError:
            await self._session.rollback()
            existing = await self.get_by_idempotency_key(idempotency_key)
            if existing is None:  # pragma: no cover - some other constraint failed
                raise
            if existing.request_fingerprint != fingerprint:
                raise IdempotencyConflictError(idempotency_key) from None
            return CreateResult(payment=existing, replayed=True)

        return CreateResult(payment=payment, replayed=False)

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        result = await self._session.execute(select(Payment).where(Payment.idempotency_key == key))
        return result.scalar_one_or_none()
