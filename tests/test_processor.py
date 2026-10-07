import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from app.consumer.gateway import GatewayResult
from app.consumer.processor import PaymentNotFoundError, PaymentProcessor
from app.consumer.webhook import WebhookDeliveryError
from app.enums import PaymentStatus
from app.models import Payment


class InMemoryRepository:
    def __init__(self, *payments: Payment) -> None:
        self.rows = {p.id: p for p in payments}

    async def get(self, payment_id):
        return self.rows.get(payment_id)

    async def finalize(self, payment_id, result):
        row = self.rows[payment_id]
        if row.status == PaymentStatus.PENDING:
            row.status = result.status
            row.failure_reason = result.failure_reason
            row.processed_at = datetime.now(UTC)
        return row

    async def mark_webhook_delivered(self, payment_id):
        self.rows[payment_id].webhook_delivered_at = datetime.now(UTC)
        self.rows[payment_id].last_error = None

    async def record_error(self, payment_id, error):
        self.rows[payment_id].last_error = error


class FakeGateway:
    def __init__(self, result: GatewayResult) -> None:
        self.result = result
        self.calls = 0

    async def charge(self, payment):
        self.calls += 1
        return self.result


class FakeWebhooks:
    def __init__(self, fail_times: int = 0) -> None:
        self.fail_times = fail_times
        self.sent: list[tuple[str, dict[str, Any]]] = []

    async def send(self, url, payload):
        if self.fail_times > 0:
            self.fail_times -= 1
            raise WebhookDeliveryError("HTTP 500")
        self.sent.append((url, payload))


async def test_success_path(payment):
    repo = InMemoryRepository(payment)
    gateway = FakeGateway(GatewayResult(PaymentStatus.SUCCEEDED))
    webhooks = FakeWebhooks()

    result = await PaymentProcessor(repo, gateway, webhooks).process(payment.id)

    assert result.status == PaymentStatus.SUCCEEDED
    assert result.processed_at is not None
    assert result.webhook_delivered_at is not None
    assert gateway.calls == 1
    url, payload = webhooks.sent[0]
    assert url == payment.webhook_url
    assert payload["event"] == "payment.processed"
    assert payload["payment"]["payment_id"] == str(payment.id)
    assert payload["payment"]["status"] == "succeeded"
    assert payload["payment"]["amount"] == "100.00"
    assert payload["payment"]["metadata"] == {"order_id": 1}


async def test_declined_payment_is_failed_and_still_notified(payment):
    repo = InMemoryRepository(payment)
    gateway = FakeGateway(GatewayResult(PaymentStatus.FAILED, failure_reason="declined"))
    webhooks = FakeWebhooks()

    result = await PaymentProcessor(repo, gateway, webhooks).process(payment.id)

    assert result.status == PaymentStatus.FAILED
    assert result.failure_reason == "declined"
    assert webhooks.sent[0][1]["payment"]["failure_reason"] == "declined"


async def test_redelivery_after_webhook_failure_does_not_charge_twice(payment):
    repo = InMemoryRepository(payment)
    gateway = FakeGateway(GatewayResult(PaymentStatus.SUCCEEDED))
    webhooks = FakeWebhooks(fail_times=1)
    processor = PaymentProcessor(repo, gateway, webhooks)

    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment.id)
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.webhook_delivered_at is None
    assert payment.last_error == "HTTP 500"

    # second delivery of the same message: gateway is skipped, webhook is resent
    await processor.process(payment.id)
    assert gateway.calls == 1
    assert len(webhooks.sent) == 1
    assert payment.webhook_delivered_at is not None
    assert payment.last_error is None


async def test_fully_processed_payment_is_a_noop(payment):
    payment.status = PaymentStatus.SUCCEEDED
    payment.webhook_delivered_at = datetime.now(UTC)
    gateway = FakeGateway(GatewayResult(PaymentStatus.FAILED))
    webhooks = FakeWebhooks()

    await PaymentProcessor(InMemoryRepository(payment), gateway, webhooks).process(payment.id)

    assert gateway.calls == 0
    assert webhooks.sent == []
    assert payment.status == PaymentStatus.SUCCEEDED


async def test_unknown_payment_is_non_retryable():
    processor = PaymentProcessor(
        InMemoryRepository(), FakeGateway(GatewayResult(PaymentStatus.SUCCEEDED)), FakeWebhooks()
    )
    with pytest.raises(PaymentNotFoundError):
        await processor.process(uuid.uuid4())
