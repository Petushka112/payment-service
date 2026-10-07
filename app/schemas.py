"""Pydantic v2 request/response models."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from app.enums import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2, examples=["1500.00"])
    currency: Currency
    description: str = Field(default="", max_length=255)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentAccepted(BaseModel):
    """Response body for POST /payments (202 Accepted)."""

    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    failure_reason: str | None = None
    last_error: str | None = None
    created_at: datetime
    updated_at: datetime
    processed_at: datetime | None = None
    webhook_delivered_at: datetime | None = None


class ErrorResponse(BaseModel):
    detail: str
