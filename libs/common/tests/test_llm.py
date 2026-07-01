from __future__ import annotations

import pytest

from sentinelchain_common import (
    ChatMessage,
    FinishReason,
    LLMProvider,
    LLMRequest,
    MockProvider,
    Role,
)


def _request(**overrides: object) -> LLMRequest:
    base: dict[str, object] = {
        "request_id": "req-1",
        "messages": [ChatMessage(role=Role.USER, content="hello")],
    }
    base.update(overrides)
    return LLMRequest(**base)  # type: ignore[arg-type]


def test_mock_provider_satisfies_protocol() -> None:
    provider = MockProvider()
    assert isinstance(provider, LLMProvider)


@pytest.mark.asyncio
async def test_mock_returns_canned_parsed_for_schema_request() -> None:
    provider = MockProvider(canned_parsed={"answer": "yes", "claims": []})
    resp = await provider.generate(_request(response_schema={"type": "object"}))
    assert resp.parsed == {"answer": "yes", "claims": []}
    assert resp.finish_reason is FinishReason.STOP
    assert resp.request_id == "req-1"


@pytest.mark.asyncio
async def test_mock_responder_callback() -> None:
    provider = MockProvider(responder=lambda req: req.messages[-1].content.upper())
    resp = await provider.generate(_request())
    assert resp.content == "HELLO"
    assert resp.parsed is None


@pytest.mark.asyncio
async def test_mock_health() -> None:
    assert await MockProvider().health() is True
