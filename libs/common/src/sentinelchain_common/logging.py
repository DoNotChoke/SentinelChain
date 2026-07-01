"""Structured JSON logging (PLAN §26).

Logs are emitted as one JSON object per line with stable keys (timestamp, level, service,
trace_id, ...). ``bind_context`` lets call sites attach per-request fields such as
``trace_id`` and ``event_id`` that then appear on every log line.
"""

from __future__ import annotations

import logging
from typing import Any

import structlog


def configure_logging(service: str, level: str = "INFO") -> None:
    """Configure structlog + stdlib logging to emit JSON lines.

    Idempotent: safe to call once at service startup.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(format="%(message)s", level=log_level)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.EventRenamer("message"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    structlog.contextvars.bind_contextvars(service=service)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a bound structlog logger."""
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    return logger


def bind_context(**kwargs: Any) -> None:
    """Bind fields (e.g. trace_id, event_id) to the current context."""
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    """Clear all context-bound fields (call at the end of a request/message)."""
    structlog.contextvars.clear_contextvars()
