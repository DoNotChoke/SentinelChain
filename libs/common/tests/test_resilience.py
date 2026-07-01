from __future__ import annotations

import pytest

from sentinelchain_common import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
    RetryPolicy,
    operation_id,
    retry_async,
)
from sentinelchain_common.resilience import RetryPolicy as RP


@pytest.mark.asyncio
async def test_retry_succeeds_after_transient_failures() -> None:
    calls = {"n": 0}

    async def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("transient")
        return "ok"

    policy = RetryPolicy(max_attempts=5, base_delay_seconds=0.0, jitter=False)
    result = await retry_async(flaky, policy)
    assert result == "ok"
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_retry_reraises_after_exhaustion() -> None:
    async def always_fail() -> str:
        raise KeyError("boom")

    policy = RP(max_attempts=2, base_delay_seconds=0.0, jitter=False, retry_on=(KeyError,))
    with pytest.raises(KeyError):
        await retry_async(always_fail, policy)


@pytest.mark.asyncio
async def test_circuit_breaker_opens_after_threshold() -> None:
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout_seconds=100.0)

    async def fail() -> None:
        raise RuntimeError("down")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(fail)

    assert breaker.state is CircuitState.OPEN
    with pytest.raises(CircuitBreakerOpenError):
        await breaker.call(fail)


@pytest.mark.asyncio
async def test_circuit_breaker_closes_on_success() -> None:
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout_seconds=0.0)

    async def fail() -> None:
        raise RuntimeError("down")

    async def ok() -> str:
        return "ok"

    with pytest.raises(RuntimeError):
        await breaker.call(fail)
    assert breaker.state is CircuitState.OPEN

    # reset_timeout is 0 -> next call is allowed (HALF_OPEN); success closes it.
    assert await breaker.call(ok) == "ok"
    assert breaker.state is CircuitState.CLOSED


def test_operation_id_is_deterministic() -> None:
    a = operation_id(["chunk-1", "bge-m3", "embed"])
    b = operation_id(["chunk-1", "bge-m3", "embed"])
    c = operation_id(["chunk-1", "bge-m3", "score"])
    assert a == b
    assert a != c
