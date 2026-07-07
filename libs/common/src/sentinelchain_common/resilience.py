"""Resilience primitives: exponential-backoff retry and a circuit breaker.

Every external call (HTTP source, embedding, LLM, OpenSearch) must have timeout + retry +
circuit breaker. These helpers provide the retry/breaker half; timeouts are applied by the
caller (e.g. ``asyncio.timeout`` or the client's own timeout).
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from enum import StrEnum

from .time import utcnow


@dataclass(slots=True)
class RetryPolicy:
    """Exponential backoff with full jitter."""

    max_attempts: int = 4
    base_delay_seconds: float = 0.2
    max_delay_seconds: float = 10.0
    jitter: bool = True
    retry_on: tuple[type[BaseException], ...] = (Exception,)

    def delay_for(self, attempt: int) -> float:
        """Delay before the given (1-based) attempt's retry."""
        raw = min(self.base_delay_seconds * (2 ** (attempt - 1)), self.max_delay_seconds)
        if self.jitter:
            return float(random.uniform(0, raw))
        return float(raw)


async def retry_async[T](
    func: Callable[[], Awaitable[T]],
    policy: RetryPolicy | None = None,
    *,
    on_retry: Callable[[int, BaseException], None] | None = None,
) -> T:
    """Invoke ``func`` with retries according to ``policy``.

    Re-raises the last exception once attempts are exhausted.
    """
    policy = policy or RetryPolicy()
    last_exc: BaseException | None = None
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return await func()
        except policy.retry_on as exc:
            last_exc = exc
            if attempt >= policy.max_attempts:
                break
            if on_retry is not None:
                on_retry(attempt, exc)
            await asyncio.sleep(policy.delay_for(attempt))
    assert last_exc is not None
    raise last_exc


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpenError(RuntimeError):
    """Raised when a call is attempted while the breaker is OPEN."""


@dataclass(slots=True)
class CircuitBreaker:
    """A simple time-based circuit breaker.

    After ``failure_threshold`` consecutive failures the breaker OPENS and rejects calls for
    ``reset_timeout_seconds``. It then transitions to HALF_OPEN and allows a trial call; a
    success CLOSES it, a failure re-OPENS it.
    """

    failure_threshold: int = 5
    reset_timeout_seconds: float = 30.0
    excluded: tuple[type[BaseException], ...] = field(default=())

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failures: int = field(default=0, init=False)
    _opened_at: float | None = field(default=None, init=False)

    @property
    def state(self) -> CircuitState:
        return self._state

    def _now(self) -> float:
        return utcnow().timestamp()

    def _allow(self) -> None:
        if self._state is CircuitState.OPEN:
            assert self._opened_at is not None
            if self._now() - self._opened_at >= self.reset_timeout_seconds:
                self._state = CircuitState.HALF_OPEN
            else:
                raise CircuitBreakerOpenError("circuit breaker is open")

    def _on_success(self) -> None:
        self._failures = 0
        self._state = CircuitState.CLOSED
        self._opened_at = None

    def _on_failure(self) -> None:
        self._failures += 1
        if self._state is CircuitState.HALF_OPEN or self._failures >= self.failure_threshold:
            self._state = CircuitState.OPEN
            self._opened_at = self._now()

    async def call[T](self, func: Callable[[], Awaitable[T]]) -> T:
        """Run ``func`` through the breaker."""
        self._allow()
        try:
            result = await func()
        except self.excluded:
            raise
        except Exception:
            self._on_failure()
            raise
        else:
            self._on_success()
            return result


def operation_id(parts: Iterable[str]) -> str:
    """Deterministic idempotency key for non-Kafka operations (PLAN §33).

    ``operation_id = hash(input_id + model_version + operation_type)``
    """
    import hashlib

    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()
