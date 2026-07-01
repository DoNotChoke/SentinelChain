# sentinelchain-common

Shared foundations imported by every SentinelChain Python service. Keeping these in one place
guarantees consistent envelopes, logging, config, observability and resilience across the
monorepo (see [docs/IMPLEMENTATION_PLAN.md §G](../../docs/IMPLEMENTATION_PLAN.md)).

## Modules

| Module | Purpose |
|---|---|
| `envelope` | Canonical `EventEnvelope` for all Kafka messages (PLAN §9), dedup key + payload hash |
| `config` | `BaseServiceSettings` (pydantic-settings, env-driven) |
| `logging` | Structured JSON logging with context binding (PLAN §26) |
| `time` | Timezone-aware UTC helpers (PLAN §36.11) |
| `resilience` | Retry with backoff, circuit breaker, idempotency `operation_id` (PLAN §33) |
| `llm` | `LLMProvider` abstraction + `MockProvider` (IMPLEMENTATION_PLAN §D) |
| `observability` | Prometheus metrics + optional OTel tracing (PLAN §26) |
| `health` | Liveness/readiness registry (PLAN §23) |
| `kafka` | Idempotent producer config + thin envelope producer (PLAN §33) |

## Install (editable, for development)

```bash
pip install -e "libs/common[dev]"
# optional extras:
pip install -e "libs/common[dev,kafka,otel]"
```

## Test

```bash
pytest libs/common/tests
```
