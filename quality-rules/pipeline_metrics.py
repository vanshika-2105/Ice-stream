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
@dataclass
class PerformanceSnapshot:
    """Represent a point-in-time pipeline performance snapshot."""

    timestamp: str
    events_received: int
    events_processed: int
    throughput_eps: float
    average_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    dlq_rate: float
    status: str
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
def calculate_latency_percentile(
    latencies_ms: list[float],
    percentile: float,
) -> float:
    """Calculate a latency percentile using linear interpolation."""

    if not latencies_ms:
        return 0.0

    if percentile < 0 or percentile > 100:
        raise ValueError("percentile must be between 0 and 100")

    sorted_latencies = sorted(latencies_ms)

    if len(sorted_latencies) == 1:
        return round(sorted_latencies[0], 2)

    position = (
        (percentile / 100)
        * (len(sorted_latencies) - 1)
    )

    lower_index = int(position)
    upper_index = min(
        lower_index + 1,
        len(sorted_latencies) - 1,
    )

    fraction = position - lower_index

    value = (
        sorted_latencies[lower_index]
        + fraction
        * (
            sorted_latencies[upper_index]
            - sorted_latencies[lower_index]
        )
    )

    return round(value, 2)


def calculate_latency_percentiles(
    latencies_ms: list[float],
) -> dict:
    """Calculate P50, P95, and P99 latency."""

    return {
        "p50_latency_ms": calculate_latency_percentile(
            latencies_ms,
            50,
        ),
        "p95_latency_ms": calculate_latency_percentile(
            latencies_ms,
            95,
        ),
        "p99_latency_ms": calculate_latency_percentile(
            latencies_ms,
            99,
        ),
    }
def build_performance_snapshot(
    timestamp: str,
    events_received: int,
    events_processed: int,
    latencies_ms: list[float],
    elapsed_seconds: float,
    dlq_events: int,
) -> PerformanceSnapshot:
    """Build a point-in-time performance snapshot."""

    throughput = calculate_throughput(
        events_processed,
        elapsed_seconds,
    )

    average_latency = calculate_average_latency(
        latencies_ms,
    )

    percentiles = calculate_latency_percentiles(
        latencies_ms,
    )

    dlq_rate = calculate_dlq_rate(
        dlq_events,
        events_received,
    )

    status = get_pipeline_status(
        throughput,
        average_latency,
    )

    return PerformanceSnapshot(
        timestamp=timestamp,
        events_received=events_received,
        events_processed=events_processed,
        throughput_eps=throughput,
        average_latency_ms=average_latency,
        p50_latency_ms=percentiles["p50_latency_ms"],
        p95_latency_ms=percentiles["p95_latency_ms"],
        p99_latency_ms=percentiles["p99_latency_ms"],
        dlq_rate=dlq_rate,
        status=status,
    )
def performance_snapshot_to_dict(
    snapshot: PerformanceSnapshot,
) -> dict:
    """Convert a performance snapshot into an API-friendly dictionary."""

    return {
        "timestamp": snapshot.timestamp,
        "events_received": snapshot.events_received,
        "events_processed": snapshot.events_processed,
        "throughput_eps": snapshot.throughput_eps,
        "average_latency_ms": snapshot.average_latency_ms,
        "p50_latency_ms": snapshot.p50_latency_ms,
        "p95_latency_ms": snapshot.p95_latency_ms,
        "p99_latency_ms": snapshot.p99_latency_ms,
        "dlq_rate": snapshot.dlq_rate,
        "status": snapshot.status,
    }