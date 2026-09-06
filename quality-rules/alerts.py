from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


HEALTHY_THRESHOLD = 95.0
WARNING_THRESHOLD = 90.0


@dataclass
class QualityAlert:
    """Represent a data-quality alert or recovery event."""

    type: str
    severity: Optional[str]
    quality_score: float
    message: str
    timestamp: str
    previous_status: Optional[str] = None
    current_status: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert the alert to a WebSocket-friendly dictionary."""

        data = {
            "type": self.type,
            "quality_score": self.quality_score,
            "message": self.message,
            "timestamp": self.timestamp,
        }

        if self.severity is not None:
            data["severity"] = self.severity

        if self.previous_status is not None:
            data["previous_status"] = self.previous_status

        if self.current_status is not None:
            data["current_status"] = self.current_status

        return data


class AlertEngine:
    """Determine quality status and generate state-transition alerts."""

    def __init__(self):
        self.current_status = "HEALTHY"

    @staticmethod
    def get_status(quality_score: float) -> str:
        """Return HEALTHY, WARNING, or CRITICAL."""

        if quality_score >= HEALTHY_THRESHOLD:
            return "HEALTHY"

        if quality_score >= WARNING_THRESHOLD:
            return "WARNING"

        return "CRITICAL"

    def evaluate(
        self,
        quality_score: float,
        invalid_event_rate: float = 0.0,
    ) -> Optional[QualityAlert]:
        """
        Evaluate the current quality score.

        Returns an alert only when the quality state changes.

        Healthy → Healthy:
            No alert

        Healthy → Warning:
            WARNING alert

        Warning → Warning:
            No duplicate alert

        Warning → Critical:
            CRITICAL alert

        Critical → Healthy:
            Recovery event
        """

        new_status = self.get_status(quality_score)
        previous_status = self.current_status

        # No state change means no new alert.
        if new_status == previous_status:
            return None

        self.current_status = new_status

        timestamp = datetime.now(timezone.utc).isoformat()

        # Recovery from WARNING or CRITICAL to HEALTHY.
        if new_status == "HEALTHY":
            return QualityAlert(
                type="QUALITY_RECOVERY",
                severity=None,
                quality_score=quality_score,
                message="Data quality has recovered",
                timestamp=timestamp,
                previous_status=previous_status,
                current_status=new_status,
            )

        # Entered WARNING state.
        if new_status == "WARNING":
            return QualityAlert(
                type="QUALITY_ALERT",
                severity="WARNING",
                quality_score=quality_score,
                message="Data quality entered warning range",
                timestamp=timestamp,
            )

        # Entered CRITICAL state.
        return QualityAlert(
            type="QUALITY_ALERT",
            severity="CRITICAL",
            quality_score=quality_score,
            message="Data quality dropped below critical threshold",
            timestamp=timestamp,
        )