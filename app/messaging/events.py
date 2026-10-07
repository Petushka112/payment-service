"""Message contracts exchanged through RabbitMQ."""

import uuid
from datetime import datetime

from pydantic import BaseModel


class PaymentCreatedEvent(BaseModel):
    """Body of ``payments.new`` messages. Deliberately minimal: the consumer reads the
    authoritative state from the database instead of trusting the message."""

    payment_id: uuid.UUID
    occurred_at: datetime | None = None
