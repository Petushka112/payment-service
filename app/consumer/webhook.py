"""Webhook delivery to the client's URL."""

import hashlib
import hmac
import json
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

import httpx
from pydantic import BaseModel, ConfigDict, Field

from app.enums import Currency, PaymentStatus
from app.models import Payment

WEBHOOK_EVENT = "payment.processed"


class WebhookDeliveryError(Exception):
    """Transport error or non-2xx answer from the client. Retryable."""


class WebhookPayment(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias="id")
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="metadata_")
    failure_reason: str | None = None
    created_at: datetime
    processed_at: datetime | None = None


class WebhookPayload(BaseModel):
    event: str = WEBHOOK_EVENT
    payment: WebhookPayment


def build_payload(payment: Payment) -> dict[str, Any]:
    return WebhookPayload(payment=WebhookPayment.model_validate(payment)).model_dump(mode="json")


def sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


class WebhookSender(Protocol):
    async def send(self, url: str, payload: dict[str, Any]) -> None: ...


class HttpWebhookSender:
    def __init__(self, client: httpx.AsyncClient, *, secret: str = "") -> None:
        self._client = client
        self._secret = secret

    async def send(self, url: str, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "payment-service-webhook/1.0",
            "X-Webhook-Event": payload.get("event", WEBHOOK_EVENT),
        }
        if self._secret:
            headers["X-Webhook-Signature"] = sign(self._secret, body)
        try:
            response = await self._client.post(url, content=body, headers=headers)
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"{type(exc).__name__}: {exc}") from exc
        if not response.is_success:
            raise WebhookDeliveryError(f"HTTP {response.status_code} from {url}")
