"""FastAPI application entrypoint (HTTP API only; no broker connection here)."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text

from app.api.payments import router as payments_router
from app.config import get_settings
from app.db import dispose_engine, get_engine
from app.logging import configure_logging


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    configure_logging(get_settings().log_level)
    yield
    await dispose_engine()


app = FastAPI(
    title="Payment Processing Service",
    version="1.0.0",
    description=(
        "Asynchronous payment processing: the API persists a payment together with an outbox "
        "event, the relay publishes it to RabbitMQ, and the consumer processes it and notifies "
        "the client via webhook."
    ),
    lifespan=lifespan,
)
app.include_router(payments_router)


@app.get("/health", tags=["system"], include_in_schema=False)
async def health() -> dict[str, str]:
    """Liveness/readiness probe for the orchestrator; intentionally unauthenticated."""
    async with get_engine().connect() as conn:
        await conn.execute(text("SELECT 1"))
    return {"status": "ok"}
