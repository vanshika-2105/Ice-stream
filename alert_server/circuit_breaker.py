from enum import Enum
from datetime import datetime, timezone


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    FAILURE_THRESHOLD = 5
    RECOVERY_TIMEOUT = 30

    def __init__(
        self,
        failure_threshold: int = FAILURE_THRESHOLD,
        recovery_timeout: int = RECOVERY_TIMEOUT,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None

    def record_success(self):
        """Record a successful dependency call."""
        self.failure_count = 0
        self.state = CircuitState.CLOSED

    def record_failure(self):
        """Record a failed dependency call."""
        self.failure_count += 1
        self.last_failure_time = datetime.now(timezone.utc)

        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN

    def can_execute(self) -> bool:
        """
        Determine whether a dependency call is allowed.

        CLOSED   -> allow
        OPEN     -> block until recovery timeout
        HALF_OPEN -> allow one recovery attempt
        """
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            if self.last_failure_time is None:
                return False

            elapsed = (
                datetime.now(timezone.utc) - self.last_failure_time
            ).total_seconds()

            if elapsed >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                return True

            return False

        if self.state == CircuitState.HALF_OPEN:
            return True

        return False