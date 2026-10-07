"""ORM models: payments and the transactional outbox."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from sqlalchemy import DateTime, Enum, Index, Integer, Numeric, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.enums import Currency, PaymentStatus


class Base(DeclarativeBase):
    pass


def _enum_values(enum_cls: type[StrEnum]) -> list[str]:
    # Store enum *values* ("pending"), not member names ("PENDING"), in the PG enum type.
    return [member.value for member in enum_cls]


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[Currency] = mapped_column(
        Enum(Currency, name="currency", native_enum=True, values_callable=_enum_values), nullable=False
    )
    description: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    # "metadata" is reserved by DeclarativeBase, so the attribute is named metadata_
    # while the DB column keeps the natural name.
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, nullable=False, default=dict)
    status: Mapped[PaymentStatus] = mapped_column(
        Enum(PaymentStatus, name="payment_status", native_enum=True, values_callable=_enum_values),
        nullable=False,
        default=PaymentStatus.PENDING,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    # SHA-256 of the canonical request body: detects the same key reused with a different payload.
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    webhook_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    # Business outcome of a declined payment (status=failed).
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Last technical error seen by the consumer (webhook delivery, etc.). Diagnostic only.
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (Index("ix_payments_status_created_at", "status", "created_at"),)


class OutboxEvent(Base):
    """Transactional outbox row.

    Written in the same transaction as the business change and published to
    RabbitMQ asynchronously by the relay (see app/outbox/relay.py).
    """

    __tablename__ = "outbox"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    # Deliberately no FK: the outbox is generic (aggregate_type + aggregate_id) and must
    # not depend on the lifecycle of any particular aggregate table.
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    # Doubles as the RabbitMQ routing key, e.g. "payments.new".
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    publish_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        # The relay only scans unpublished rows, so a small partial index is enough.
        Index("ix_outbox_unpublished", "created_at", postgresql_where="published_at IS NULL"),
    )
