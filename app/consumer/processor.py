"""Payment processing state machine executed by the consumer.

Every step is idempotent with respect to message redelivery:

1. ``pending`` -> call the gateway -> ``succeeded``/``failed`` (compare-and-set on status,
   so a duplicate delivery can never overwrite a final outcome);
2. webhook not yet delivered -> deliver it -> ``webhook_delivered_at`` set.

A redelivered message therefore resumes from wherever the previous attempt stopped
instead of charging the customer or notifying the client twice.
"""

import logging
import uuid

from app.consumer.gateway import PaymentGateway
from app.consumer.repository import PaymentRepository
from app.consumer.webhook import WebhookDeliveryError, WebhookSender, build_payload
from app.enums import PaymentStatus
from app.models import Payment

log = logging.getLogger(__name__)


class NonRetryableError(Exception):
    """Processing can never succeed for this message: it goes straight to the DLQ."""


class PaymentNotFoundError(NonRetryableError):
    pass


class PaymentProcessor:
    def __init__(
        self, repository: PaymentRepository, gateway: PaymentGateway, webhooks: WebhookSender
    ) -> None:
        self._repo = repository
        self._gateway = gateway
        self._webhooks = webhooks

    async def process(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._repo.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(f"Payment {payment_id} does not exist")

        if payment.status == PaymentStatus.PENDING:
            log.info("Charging payment %s (%s %s)", payment.id, payment.amount, payment.currency)
            result = await self._gateway.charge(payment)
            payment = await self._repo.finalize(payment.id, result)
            log.info("Payment %s -> %s", payment.id, payment.status)
        else:
            log.info("Payment %s already %s, skipping gateway", payment.id, payment.status)

        if payment.webhook_delivered_at is None:
            try:
                await self._webhooks.send(payment.webhook_url, build_payload(payment))
            except WebhookDeliveryError as exc:
                await self._repo.record_error(payment.id, str(exc))
                raise
            await self._repo.mark_webhook_delivered(payment.id)
            log.info("Webhook for payment %s delivered to %s", payment.id, payment.webhook_url)

        return payment
