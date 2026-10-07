import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.enums import Currency, PaymentStatus
from app.models import Payment


def make_payment(**overrides) -> Payment:
    now = datetime.now(UTC)
    defaults = dict(
        id=uuid.uuid4(),
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="test",
        metadata_={"order_id": 1},
        status=PaymentStatus.PENDING,
        idempotency_key=str(uuid.uuid4()),
        request_fingerprint="f" * 64,
        webhook_url="http://client.test/webhook",
        created_at=now,
        updated_at=now,
    )
    defaults.update(overrides)
    return Payment(**defaults)


@pytest.fixture
def payment() -> Payment:
    return make_payment()
