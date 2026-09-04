from collections import Counter


class QualityMetrics:
    """Track data-quality metrics for processed events."""

    def __init__(self):
        self.total_events = 0
        self.valid_events = 0
        self.invalid_events = 0
        self.error_counts = Counter()

    def record_valid(self):
        """Record a valid event."""
        self.total_events += 1
        self.valid_events += 1

    def record_invalid(self, errors):
        """Record an invalid event and its validation errors."""
        self.total_events += 1
        self.invalid_events += 1

        for error in errors:
            code = error.get("code")

            if code:
                self.error_counts[code] += 1

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
            "valid_events": self.valid_events,
            "invalid_events": self.invalid_events,
            "quality_score": round(self.quality_score, 2),
            "invalid_event_rate": round(self.invalid_event_rate, 2),
            "error_counts": error_counts,

            # Backward compatibility with Day 6 API/tests
            "errors": error_counts,
        }