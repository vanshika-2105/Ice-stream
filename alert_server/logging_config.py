import logging
from collections import deque
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any


REQUEST_ID: ContextVar[str] = ContextVar("request_id", default="-")


class ObservabilityEventStore(logging.Handler):
    """Store a bounded history of application log events."""

    MAX_EVENTS = 500

    def __init__(self) -> None:
        super().__init__()
        self.events: deque[dict[str, Any]] = deque(maxlen=self.MAX_EVENTS)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            event = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": record.levelname,
                "component": getattr(record, "component", "backend"),
                "message": record.getMessage(),
                "request_id": getattr(
                    record,
                    "request_id",
                    REQUEST_ID.get(),
                ),
            }

            self.events.append(event)
        except Exception:
            # Logging must never break the application.
            self.handleError(record)

    def get_recent(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return the most recent log events."""
        limit = max(1, min(limit, self.MAX_EVENTS))
        events = list(self.events)
        return events[-limit:]


observability_store = ObservabilityEventStore()


class RequestContextFilter(logging.Filter):
    """Attach the current request ID to every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = REQUEST_ID.get()
        return True


def configure_logging() -> logging.Logger:
    """Configure centralized backend logging."""

    logger = logging.getLogger("ice_stream")

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | "
        "request_id=%(request_id)s | "
        "component=%(component)s | %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.addFilter(RequestContextFilter())

    observability_store.setFormatter(formatter)
    observability_store.addFilter(RequestContextFilter())

    logger.addHandler(console_handler)
    logger.addHandler(observability_store)

    return logger


logger = configure_logging()


def set_request_id(request_id: str) -> None:
    """Set the correlation ID for the current execution context."""
    REQUEST_ID.set(request_id)


def clear_request_id() -> None:
    """Clear the correlation ID for the current execution context."""
    REQUEST_ID.set("-")


def get_recent_logs(limit: int = 100) -> list[dict[str, Any]]:
    """Return recent bounded observability events."""
    return observability_store.get_recent(limit)
