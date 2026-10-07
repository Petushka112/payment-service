"""Application settings loaded from environment variables / .env file."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- infrastructure -------------------------------------------------
    database_url: str = Field(
        default="postgresql+asyncpg://payments:payments@localhost:5432/payments",
        description="SQLAlchemy async DSN",
    )
    rabbitmq_url: str = Field(default="amqp://guest:guest@localhost:5672/")

    # --- security -------------------------------------------------------
    api_key: str = Field(default="change-me", description="Static API key for X-API-Key header")
    webhook_secret: str = Field(
        default="",
        description="If set, webhooks are signed with HMAC-SHA256 (X-Webhook-Signature header)",
    )

    # --- outbox relay ---------------------------------------------------
    outbox_poll_interval: float = Field(default=0.5, gt=0, description="Seconds between outbox polls")
    outbox_batch_size: int = Field(default=100, gt=0)

    # --- consumer / retry -----------------------------------------------
    consumer_prefetch: int = Field(default=10, gt=0)
    max_attempts: int = Field(default=3, ge=1, description="Total processing attempts before DLQ")
    retry_base_delay: float = Field(default=2.0, gt=0, description="Delay before 1st retry, seconds")
    retry_backoff_factor: float = Field(default=2.0, ge=1, description="Exponential multiplier")

    # --- payment gateway emulation --------------------------------------
    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    # --- webhook delivery -----------------------------------------------
    webhook_timeout: float = Field(default=5.0, gt=0, description="HTTP timeout per webhook call")

    log_level: str = Field(default="INFO")


@lru_cache
def get_settings() -> Settings:
    return Settings()
