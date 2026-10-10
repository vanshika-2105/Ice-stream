from dataclasses import dataclass
from datetime import datetime


@dataclass
class LiveMetrics:
    """Represent the latest live streaming metrics."""

    total_events: int
    valid_events: int
    invalid_events: int
    throughput_eps: float
    average_latency_ms: float
    dlq_rate: float
    quality_percentage: float
    last_event_time: datetime | None
    status: str
