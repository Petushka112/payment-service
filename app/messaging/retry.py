"""Retry policy: N attempts with exponential back-off between them."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3  # total attempts, including the first one
    base_delay: float = 2.0  # seconds before the first retry
    backoff_factor: float = 2.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay <= 0 or self.backoff_factor < 1:
            raise ValueError("base_delay must be > 0 and backoff_factor >= 1")

    def should_retry(self, attempt: int) -> bool:
        """``attempt`` is the 1-based number of the attempt that just failed."""
        return attempt < self.max_attempts

    def delay_for(self, attempt: int) -> float:
        """Delay (seconds) to wait after the given failed attempt: base * factor^(attempt-1)."""
        return self.base_delay * self.backoff_factor ** (attempt - 1)

    @property
    def retry_attempts(self) -> range:
        """Attempt numbers after which a retry is scheduled (1 .. max_attempts-1)."""
        return range(1, self.max_attempts)
