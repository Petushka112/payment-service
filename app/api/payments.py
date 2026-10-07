import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.auth import require_api_key
from app.db import get_session
from app.schemas import ErrorResponse, PaymentAccepted, PaymentCreate, PaymentRead
from app.services.payments import IdempotencyConflictError, PaymentService

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
    responses={401: {"model": ErrorResponse, "description": "Invalid or missing API key"}},
)


def get_payment_service(session: AsyncSession = Depends(get_session)) -> PaymentService:
    return PaymentService(session)


ServiceDep = Annotated[PaymentService, Depends(get_payment_service)]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    summary="Create a payment (asynchronous)",
    responses={409: {"model": ErrorResponse, "description": "Idempotency-Key reused with another body"}},
)
async def create_payment(
    data: PaymentCreate,
    response: Response,
    service: ServiceDep,
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key", min_length=1, max_length=255, description="Client-generated unique key"
        ),
    ],
) -> PaymentAccepted:
    try:
        result = await service.create(data, idempotency_key)
    except IdempotencyConflictError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        ) from None

    if result.replayed:
        # Same key + same body -> same answer as the first time, flagged for the client.
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAccepted.model_validate(result.payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    summary="Get payment details",
    responses={404: {"model": ErrorResponse, "description": "Payment not found"}},
)
async def get_payment(payment_id: uuid.UUID, service: ServiceDep) -> PaymentRead:
    payment = await service.get(payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentRead.model_validate(payment)
