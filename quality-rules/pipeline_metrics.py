from dataclasses import dataclass
MAX_LATENCY_WARNING_MS = 500.0
MAX_LATENCY_CRITICAL_MS = 1000.0

MIN_THROUGHPUT_WARNING_EPS = 5.0
MIN_THROUGHPUT_CRITICAL_EPS = 1.0

@dataclass
class PipelineMetrics:
    """Represent pipeline performance metrics."""

    total_events: int
    valid_events: int
    invalid_events: int
    throughput_eps: float
    average_latency_ms: float
    dlq_rate: float
def calculate_throughput(
    total_events: int,
    elapsed_seconds: float,
) -> float:
    """Calculate events processed per second."""

    if elapsed_seconds <= 0:
        return 0.0

    return round(total_events / elapsed_seconds, 2)
def calculate_average_latency(
    latencies_ms: list[float],
) -> float:
    """Calculate average processing latency in milliseconds."""

    if not latencies_ms:
        return 0.0

    return round(sum(latencies_ms) / len(latencies_ms), 2)
def calculate_dlq_rate(
    invalid_events: int,
    total_events: int,
) -> float:
    """Calculate the percentage of events sent to the DLQ."""

    if total_events == 0:
        return 0.0

    return round((invalid_events / total_events) * 100, 2)
def build_pipeline_metrics(
    total_events: int,
    valid_events: int,
    invalid_events: int,
    elapsed_seconds: float,
    latencies_ms: list[float],
) -> PipelineMetrics:
    """Build complete pipeline performance metrics."""

    throughput = calculate_throughput(
        total_events,
        elapsed_seconds,
    )

    average_latency = calculate_average_latency(
        latencies_ms,
    )

    dlq_rate = calculate_dlq_rate(
        invalid_events,
        total_events,
    )

    return PipelineMetrics(
        total_events=total_events,
        valid_events=valid_events,
        invalid_events=invalid_events,
        throughput_eps=throughput,
        average_latency_ms=average_latency,
        dlq_rate=dlq_rate,
    )
def get_pipeline_status(
    throughput_eps: float,
    average_latency_ms: float,
) -> str:
    """Determine the current pipeline performance status."""

    if (
        throughput_eps < MIN_THROUGHPUT_CRITICAL_EPS
        or average_latency_ms > MAX_LATENCY_CRITICAL_MS
    ):
        return "CRITICAL"

    if (
        throughput_eps < MIN_THROUGHPUT_WARNING_EPS
        or average_latency_ms > MAX_LATENCY_WARNING_MS
    ):
        return "DEGRADED"

    return "HEALTHY"
def pipeline_metrics_to_dict(
    metrics: PipelineMetrics,
) -> dict:
    """Convert pipeline metrics into an API-friendly dictionary."""

    return {
        "total_events": metrics.total_events,
        "valid_events": metrics.valid_events,
        "invalid_events": metrics.invalid_events,
        "throughput_eps": metrics.throughput_eps,
        "average_latency_ms": metrics.average_latency_ms,
        "dlq_rate": metrics.dlq_rate,
        "status": get_pipeline_status(
            metrics.throughput_eps,
            metrics.average_latency_ms,
        ),
    }