"""External payment gateway (emulated)."""

import asyncio
import random
from dataclasses import dataclass
from typing import Protocol

from app.enums import PaymentStatus
from app.models import Payment


@dataclass(frozen=True, slots=True)
class GatewayResult:
    status: PaymentStatus
    failure_reason: str | None = None


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> GatewayResult: ...


class EmulatedGateway:
    """Takes 2-5 seconds and succeeds ~90% of the time (all configurable).

    A real gateway would receive ``payment.id`` as its idempotency key so that a
    redelivered message can never charge the customer twice.
    """

    def __init__(
        self,
        *,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
        success_rate: float = 0.9,
        rng: random.Random | None = None,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._rng = rng or random.Random()

    async def charge(self, payment: Payment) -> GatewayResult:
        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._success_rate:
            return GatewayResult(status=PaymentStatus.SUCCEEDED)
        return GatewayResult(status=PaymentStatus.FAILED, failure_reason="Declined by issuer (emulated)")
