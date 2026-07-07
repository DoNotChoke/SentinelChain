"""Observability helpers: Prometheus metrics and optional OpenTelemetry tracing.

Prometheus is a core dependency. OpenTelemetry is optional (install the ``otel`` extra); the
tracing setup imports it lazily so the package works without it.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Default latency buckets tuned for the performance targets in PLAN §34.
DEFAULT_LATENCY_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    8.0,
    10.0,
)


def counter(name: str, documentation: str, labelnames: tuple[str, ...] = ()) -> Counter:
    return Counter(name, documentation, labelnames)


def gauge(name: str, documentation: str, labelnames: tuple[str, ...] = ()) -> Gauge:
    return Gauge(name, documentation, labelnames)


def histogram(
    name: str,
    documentation: str,
    labelnames: tuple[str, ...] = (),
    buckets: tuple[float, ...] = DEFAULT_LATENCY_BUCKETS,
) -> Histogram:
    return Histogram(name, documentation, labelnames, buckets=buckets)


def start_metrics_server(port: int) -> None:
    """Expose Prometheus metrics on ``/metrics`` at the given port."""
    start_http_server(port)


def setup_tracing(service_name: str, endpoint: str) -> None:
    """Configure OTLP tracing if the ``otel`` extra is installed; otherwise no-op.

    Trace propagation flows through HTTP and Kafka headers (PLAN §26).
    """
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        return

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
