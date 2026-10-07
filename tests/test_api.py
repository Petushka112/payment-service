"""API tests with the service layer replaced by an in-memory fake (no database needed)."""

import uuid

import httpx
import pytest

from app.api.payments import get_payment_service
from app.config import Settings, get_settings
from app.main import app
from app.schemas import PaymentCreate
from app.services.payments import CreateResult, IdempotencyConflictError, request_fingerprint
from tests.conftest import make_payment

API_KEY = "test-key"
BODY = {
    "amount": "250.00",
    "currency": "USD",
    "description": "Subscription",
    "metadata": {"plan": "pro"},
    "webhook_url": "https://client.example.com/hook",
}


class FakePaymentService:
    def __init__(self) -> None:
        self.by_key = {}
        self.by_id = {}

    async def create(self, data: PaymentCreate, idempotency_key: str) -> CreateResult:
        fingerprint = request_fingerprint(data)
        if idempotency_key in self.by_key:
            existing = self.by_key[idempotency_key]
            if existing.request_fingerprint != fingerprint:
                raise IdempotencyConflictError(idempotency_key)
            return CreateResult(payment=existing, replayed=True)
        payment = make_payment(
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata_=data.metadata,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
            webhook_url=str(data.webhook_url),
        )
        self.by_key[idempotency_key] = payment
        self.by_id[payment.id] = payment
        return CreateResult(payment=payment, replayed=False)

    async def get(self, payment_id: uuid.UUID):
        return self.by_id.get(payment_id)


@pytest.fixture
def service():
    return FakePaymentService()


@pytest.fixture
async def client(service):
    app.dependency_overrides[get_payment_service] = lambda: service
    app.dependency_overrides[get_settings] = lambda: Settings(api_key=API_KEY)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test", headers={"X-API-Key": API_KEY}
    ) as c:
        yield c
    app.dependency_overrides.clear()


async def test_requires_api_key(client):
    r = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": "wrong"})
    assert r.status_code == 401
    r = await client.post("/api/v1/payments", json=BODY, headers={"X-API-Key": "", "Idempotency-Key": "k"})
    assert r.status_code == 401


async def test_create_and_get(client):
    r = await client.post("/api/v1/payments", json=BODY, headers={"Idempotency-Key": "order-1"})
    assert r.status_code == 202, r.text
    created = r.json()
    assert created["status"] == "pending"
    assert set(created) == {"payment_id", "status", "created_at"}

    r = await client.get(f"/api/v1/payments/{created['payment_id']}")
    assert r.status_code == 200
    details = r.json()
    assert details["payment_id"] == created["payment_id"]
    assert details["amount"] == "250.00"
    assert details["currency"] == "USD"
    assert details["metadata"] == {"plan": "pro"}
    assert details["idempotency_key"] == "order-1"
    assert details["processed_at"] is None


async def test_idempotency_key_is_required(client):
    r = await client.post("/api/v1/payments", json=BODY)
    assert r.status_code == 422


async def test_same_key_same_body_is_replayed(client):
    headers = {"Idempotency-Key": "order-2"}
    first = await client.post("/api/v1/payments", json=BODY, headers=headers)
    second = await client.post("/api/v1/payments", json=BODY, headers=headers)
    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert "Idempotent-Replayed" not in first.headers


async def test_same_key_different_body_conflicts(client):
    headers = {"Idempotency-Key": "order-3"}
    await client.post("/api/v1/payments", json=BODY, headers=headers)
    r = await client.post("/api/v1/payments", json={**BODY, "amount": "1.00"}, headers=headers)
    assert r.status_code == 409


async def test_validation_error(client):
    r = await client.post(
        "/api/v1/payments", json={**BODY, "currency": "GBP"}, headers={"Idempotency-Key": "k"}
    )
    assert r.status_code == 422


async def test_unknown_payment_404(client):
    r = await client.get(f"/api/v1/payments/{uuid.uuid4()}")
    assert r.status_code == 404
