from collections import Counter
from datetime import datetime, timezone


class QualityMetrics:
    """Track data-quality metrics for processed events."""

    def __init__(self):
        self.total_events = 0
        self.valid_events = 0
        self.invalid_events = 0
        self.error_counts = Counter()

        # Streaming health
        self.last_event_time = None

    def _record_event_time(self):
        """Update the timestamp of the most recently processed event."""
        self.last_event_time = datetime.now(timezone.utc).isoformat()

    def record_valid(self):
        """Record a valid event."""
        self.total_events += 1
        self.valid_events += 1
        self._record_event_time()

    def record_invalid(self, errors):
        """Record an invalid event and its validation errors."""
        self.total_events += 1
        self.invalid_events += 1

        for error in errors:
            code = error.get("code")

            if code:
                self.error_counts[code] += 1

        self._record_event_time()

    @property
    def quality_score(self):
        """Return the current quality score as a percentage."""
        if self.total_events == 0:
            return 0.0

        return (self.valid_events / self.total_events) * 100

    @property
    def invalid_event_rate(self):
        """Return the invalid event rate as a percentage."""
        if self.total_events == 0:
            return 0.0

        return (self.invalid_events / self.total_events) * 100

    def get_metrics(self):
        """Return metrics in a dashboard-friendly format."""
        error_counts = dict(self.error_counts)

        return {
            "total_events": self.total_events,
            "events_processed": self.total_events,
            "valid_events": self.valid_events,
            "invalid_events": self.invalid_events,
            "quality_score": round(self.quality_score, 2),
            "invalid_event_rate": round(self.invalid_event_rate, 2),
            "last_event_time": self.last_event_time,
            "error_counts": error_counts,

            # Backward compatibility with Day 6 API/tests
            "errors": error_counts,
        }