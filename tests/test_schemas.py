from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.schemas import PaymentCreate
from app.services.payments import request_fingerprint

VALID = {
    "amount": "1500.50",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://client.example.com/hooks/payments",
}


def test_valid_payload():
    data = PaymentCreate(**VALID)
    assert data.amount == Decimal("1500.50")
    assert data.currency == "RUB"


@pytest.mark.parametrize(
    "patch",
    [
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "10.123"},
        {"currency": "GBP"},
        {"webhook_url": "not-a-url"},
        {"description": "x" * 256},
        {"unexpected": 1},
    ],
)
def test_invalid_payload(patch):
    with pytest.raises(ValidationError):
        PaymentCreate(**{**VALID, **patch})


def test_fingerprint_ignores_key_order_but_not_values():
    a = PaymentCreate(**VALID)
    b = PaymentCreate(**dict(reversed(list(VALID.items()))))
    c = PaymentCreate(**{**VALID, "amount": "1500.51"})
    assert request_fingerprint(a) == request_fingerprint(b)
    assert request_fingerprint(a) != request_fingerprint(c)
