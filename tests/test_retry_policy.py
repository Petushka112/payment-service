import pytest

from app.messaging.retry import RetryPolicy
from app.messaging.topology import PAYMENTS_EXCHANGE, retry_queue


def test_exponential_delays():
    policy = RetryPolicy(max_attempts=4, base_delay=2, backoff_factor=2)
    assert [policy.delay_for(a) for a in policy.retry_attempts] == [2, 4, 8]


def test_should_retry_until_last_attempt():
    policy = RetryPolicy(max_attempts=3)
    assert policy.should_retry(1)
    assert policy.should_retry(2)
    assert not policy.should_retry(3)


def test_single_attempt_never_retries():
    policy = RetryPolicy(max_attempts=1)
    assert list(policy.retry_attempts) == []
    assert not policy.should_retry(1)


@pytest.mark.parametrize("kwargs", [{"max_attempts": 0}, {"base_delay": 0}, {"backoff_factor": 0.5}])
def test_invalid_policy(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


def test_retry_queue_routes_back_to_main_queue():
    queue = retry_queue(2, 4.0)
    assert queue.name == "payments.retry.2"
    assert queue.arguments["x-message-ttl"] == 4000
    assert queue.arguments["x-dead-letter-exchange"] == PAYMENTS_EXCHANGE
    assert queue.arguments["x-dead-letter-routing-key"] == "payments.new"
