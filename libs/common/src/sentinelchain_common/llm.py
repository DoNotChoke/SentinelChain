"""LLM provider abstraction"""

from __future__ import annotations

import json
from collections.abc import Callable
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field


class Role(StrEnum):
    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"


class ChatMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Role
    content: str


class ToolSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    description: str
    parameters_schema: dict[str, Any]


class TokenUsage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    prompt_tokens: int = 0
    completion_tokens: int = 0

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens


class FinishReason(StrEnum):
    STOP = "stop"
    LENGTH = "length"
    TOOL_CALL = "tool_call"
    SCHEMA_VIOLATION = "schema_violation"
    ERROR = "error"


class LLMRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str
    messages: list[ChatMessage]
    response_schema: dict[str, Any] | None = None
    max_tokens: int = 2048
    temperature: float = 0.0
    tools: list[ToolSpec] | None = None
    trace_id: str = ""


class LLMResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str
    content: str
    parsed: dict[str, Any] | None = None
    usage: TokenUsage = Field(default_factory=TokenUsage)
    model: str
    finish_reason: FinishReason = FinishReason.STOP
    latency_ms: int = 0


class ProviderCapabilities(BaseModel):
    model_config = ConfigDict(extra="forbid")
    supports_tools: bool = False
    supports_json_mode: bool = False
    max_context_tokens: int = 8192


@runtime_checkable
class LLMProvider(Protocol):
    @property
    def capabilities(self) -> ProviderCapabilities: ...

    async def generate(self, req: LLMRequest) -> LLMResponse: ...

    async def health(self) -> bool: ...


class MockProvider:
    def __init__(
        self,
        model: str = "mock-1",
        responder: Callable[[LLMRequest], str] | None = None,
        canned_parsed: dict[str, Any] | None = None,
    ) -> None:
        self._model = model
        self._responder = responder
        self._canned_parsed = canned_parsed

    @property
    def capabilities(self) -> ProviderCapabilities:
        return ProviderCapabilities(
            supports_tools=True,
            supports_json_mode=True,
            max_context_tokens=8192,
        )

    async def generate(self, req: LLMRequest) -> LLMResponse:
        if self._responder is not None:
            content = self._responder(req)
        elif req.response_schema is not None:
            content = json.dumps(self._canned_parsed or {})
        else:
            content = "mock response"

        parsed: dict[str, Any] | None = None
        if req.response_schema is not None:
            parsed = self._canned_parsed or _safe_json(content)

        return LLMResponse(
            request_id=req.request_id,
            content=content,
            parsed=parsed,
            usage=TokenUsage(prompt_tokens=len(content), completion_tokens=len(content)),
            model=self._model,
            finish_reason=FinishReason.STOP,
            latency_ms=0,
        )

    async def health(self) -> bool:
        return True


def _safe_json(text: str) -> dict[str, Any] | None:
    try:
        value = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    return value if isinstance(value, dict) else None
