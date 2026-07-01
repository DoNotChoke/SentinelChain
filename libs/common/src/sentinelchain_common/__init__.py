"""SentinelChain shared foundations.

See docs/IMPLEMENTATION_PLAN.md §G for how this package fits the monorepo.
"""

from __future__ import annotations

from .config import BaseServiceSettings
from .envelope import EventEnvelope, new_uuid
from .health import HealthRegistry
from .llm import (
    ChatMessage,
    FinishReason,
    LLMProvider,
    LLMRequest,
    LLMResponse,
    MockProvider,
    ProviderCapabilities,
    Role,
    TokenUsage,
    ToolSpec,
)
from .logging import bind_context, clear_context, configure_logging, get_logger
from .resilience import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
    RetryPolicy,
    operation_id,
    retry_async,
)
from .time import isoformat_utc, to_utc, utcnow

__version__ = "0.1.0"

__all__ = [
    "BaseServiceSettings",
    "ChatMessage",
    "CircuitBreaker",
    "CircuitBreakerOpenError",
    "CircuitState",
    "EventEnvelope",
    "FinishReason",
    "HealthRegistry",
    "LLMProvider",
    "LLMRequest",
    "LLMResponse",
    "MockProvider",
    "ProviderCapabilities",
    "RetryPolicy",
    "Role",
    "TokenUsage",
    "ToolSpec",
    "__version__",
    "bind_context",
    "clear_context",
    "configure_logging",
    "get_logger",
    "isoformat_utc",
    "new_uuid",
    "operation_id",
    "retry_async",
    "to_utc",
    "utcnow",
]
