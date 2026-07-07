from __future__ import annotations

import httpx

from sentinelchain_common import (
    CircuitBreaker,
    RetryPolicy,
    get_logger,
    retry_async,
)

from .config import IngestionUsgsSettings

_log = get_logger("ingestion-usgs.client")


class UsgsClient:
    def __init__(
        self,
        settings: IngestionUsgsSettings,
        *,
        client: httpx.AsyncClient | None = None,
        breaker: CircuitBreaker | None = None,
    ) -> None:
        self._url = settings.usgs_feed_url
        self._timeout = settings.usgs_request_timeout_seconds
        self._client = client or httpx.AsyncClient(timeout=self._timeout)
        self._breaker = breaker or CircuitBreaker(failure_threshold=5, reset_timeout_seconds=60.0)
        self._retry = RetryPolicy(
            max_attempts=settings.usgs_max_retries,
            retry_on=(httpx.HTTPError,),
        )

    async def fetch_feed(self) -> dict[str, object]:
        async def _once() -> dict[str, object]:
            response = await self._client.get(self._url)
            response.raise_for_status()
            data: dict[str, object] = response.json()
            return data

        def _on_retry(attempt: int, exc: BaseException) -> None:
            _log.warning("usgs_fetch_retry", attempt=attempt, error=str(exc))

        return await self._breaker.call(lambda: retry_async(_once, self._retry, on_retry=_on_retry))

    async def aclose(self) -> None:
        await self._client.aclose()
