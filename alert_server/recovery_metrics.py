from datetime import datetime, timezone


class RecoveryMetrics:
    """Track lightweight circuit-breaker recovery metrics."""

    def __init__(self):
        self.recovery_count = 0
        self.failure_count = 0
        self.retry_count = 0

        self.current_circuit_state = "CLOSED"

        self.last_failure_time = None
        self.last_recovery_time = None

        self.history = []

    def record_failure(self, component: str = "kafka"):
        """Record a dependency failure."""

        timestamp = datetime.now(timezone.utc).isoformat()

        self.failure_count += 1
        self.last_failure_time = timestamp

        self.history.append(
            {
                "event": "FAILURE",
                "component": component,
                "timestamp": timestamp,
            }
        )

    def record_retry(
        self,
        attempt: int,
        component: str = "kafka",
    ):
        """Record a retry attempt."""

        timestamp = datetime.now(timezone.utc).isoformat()

        self.retry_count += 1

        self.history.append(
            {
                "event": "RETRY",
                "component": component,
                "attempt": attempt,
                "timestamp": timestamp,
            }
        )

    def record_circuit_transition(
        self,
        state: str,
        component: str = "kafka",
    ):
        """Record a circuit state transition."""

        timestamp = datetime.now(timezone.utc).isoformat()

        self.current_circuit_state = state

        self.history.append(
            {
                "event": "CIRCUIT_TRANSITION",
                "component": component,
                "state": state,
                "timestamp": timestamp,
            }
        )

    def record_recovery(self, component: str = "kafka"):
        """Record a successful dependency recovery."""

        timestamp = datetime.now(timezone.utc).isoformat()

        self.recovery_count += 1
        self.last_recovery_time = timestamp

        self.history.append(
            {
                "event": "RECOVERY",
                "component": component,
                "status": "RECOVERED",
                "timestamp": timestamp,
            }
        )

    def get_metrics(self) -> dict:
        """Return recovery metrics in dashboard-friendly format."""

        return {
            "recovery_count": self.recovery_count,
            "failure_count": self.failure_count,
            "retry_count": self.retry_count,
            "current_circuit_state": self.current_circuit_state,
            "last_failure_time": self.last_failure_time,
            "last_recovery_time": self.last_recovery_time,
        }

    def get_history(self) -> list:
        """Return recovery transition history."""

        return list(self.history)