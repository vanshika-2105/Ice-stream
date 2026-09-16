from collections import deque
from datetime import datetime, timezone
import time

from live_metrics import LiveMetrics


LIVE_WINDOW_SECONDS = 10.0
STREAM_STALE_THRESHOLD_SECONDS = 10.0


class LiveAggregator:
    """Maintain rolling live metrics for the streaming pipeline."""

    def __init__(self):
        self.events = deque()

        self.total_events = 0
        self.valid_events = 0
        self.invalid_events = 0

        self.last_event_time = None

    def record_event(
        self,
        valid: bool,
        latency_ms: float,
        event_time: datetime | None = None,
    ):
        """Record one processed event."""

        now = time.monotonic()

        if event_time is None:
            event_time = datetime.now(timezone.utc)

        self.events.append(
            {
                "monotonic_time": now,
                "valid": valid,
                "latency_ms": float(latency_ms),
                "event_time": event_time,
            }
        )

        self.last_event_time = event_time

        self._remove_old_events(now)

    def _remove_old_events(self, now: float):
        """Remove events outside the rolling live window."""

        cutoff = now - LIVE_WINDOW_SECONDS

        while self.events and self.events[0]["monotonic_time"] < cutoff:
            self.events.popleft()

    def _get_current_events(self):
        """Return events currently inside the live window."""

        now = time.monotonic()
        self._remove_old_events(now)
        return list(self.events)

    def get_metrics(self) -> LiveMetrics:
        """Calculate the latest rolling live metrics."""

        current_events = self._get_current_events()

        total_events = len(current_events)
        valid_events = sum(
            1 for event in current_events if event["valid"]
        )
        invalid_events = total_events - valid_events

        if total_events > 0:
            elapsed_seconds = LIVE_WINDOW_SECONDS
            throughput_eps = round(
                total_events / elapsed_seconds,
                2,
            )

            average_latency_ms = round(
                sum(
                    event["latency_ms"]
                    for event in current_events
                ) / total_events,
                2,
            )

            dlq_rate = round(
                (invalid_events / total_events) * 100,
                2,
            )

            quality_percentage = round(
                (valid_events / total_events) * 100,
                2,
            )
        else:
            throughput_eps = 0.0
            average_latency_ms = 0.0
            dlq_rate = 0.0
            quality_percentage = 0.0

        status = self._get_status()

        return LiveMetrics(
            total_events=total_events,
            valid_events=valid_events,
            invalid_events=invalid_events,
            throughput_eps=throughput_eps,
            average_latency_ms=average_latency_ms,
            dlq_rate=dlq_rate,
            quality_percentage=quality_percentage,
            last_event_time=self.last_event_time,
            status=status,
        )

    def _get_status(self) -> str:
        """Determine the current live stream status."""

        if self.last_event_time is None:
            return "STALE"

        now = datetime.now(timezone.utc)

        last_event = self.last_event_time

        if last_event.tzinfo is None:
            last_event = last_event.replace(tzinfo=timezone.utc)

        age_seconds = (
            now - last_event
        ).total_seconds()

        if age_seconds > STREAM_STALE_THRESHOLD_SECONDS:
            return "STALE"

        metrics_events = self._get_current_events()

        if metrics_events:
            valid_events = sum(
                1 for event in metrics_events if event["valid"]
            )

            quality_percentage = (
                valid_events / len(metrics_events)
            ) * 100

            if quality_percentage < 90:
                return "CRITICAL"

            if quality_percentage < 95:
                return "DEGRADED"

        return "ACTIVE"

    def reset(self):
        """Clear all live aggregation state."""

        self.events.clear()
        self.total_events = 0
        self.valid_events = 0
        self.invalid_events = 0
        self.last_event_time = None
