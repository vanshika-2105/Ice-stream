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
    """Determine quality status and generate quality and anomaly alerts."""

    def __init__(self):
        self.current_status = "HEALTHY"
        self.anomaly_active = False
        self.anomaly_severity = "INFO"

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
        """

        new_status = self.get_status(quality_score)
        previous_status = self.current_status

        if new_status == previous_status:
            return None

        self.current_status = new_status

        timestamp = datetime.now(timezone.utc).isoformat()

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

        if new_status == "WARNING":
            return QualityAlert(
                type="QUALITY_ALERT",
                severity="WARNING",
                quality_score=quality_score,
                message="Data quality entered warning range",
                timestamp=timestamp,
            )

        return QualityAlert(
            type="QUALITY_ALERT",
            severity="CRITICAL",
            quality_score=quality_score,
            message="Data quality dropped below critical threshold",
            timestamp=timestamp,
        )

    def evaluate_anomaly(
        self,
        is_anomaly: bool,
        severity: str,
        current_quality: float,
        baseline_quality: float,
        deviation: float,
        reason: str,
    ) -> Optional[dict]:
        """
        Evaluate anomaly state and generate anomaly alerts.

        Returns an alert only when the anomaly state changes.
        """

        timestamp = datetime.now(timezone.utc).isoformat()

        # New anomaly detected.
        if is_anomaly and not self.anomaly_active:
            self.anomaly_active = True
            self.anomaly_severity = severity

            return {
                "type": "QUALITY_ANOMALY_ALERT",
                "severity": severity,
                "current_quality": current_quality,
                "baseline_quality": baseline_quality,
                "deviation": round(deviation, 2),
                "message": reason,
                "timestamp": timestamp,
            }

        # Anomaly continues.
        if is_anomaly and self.anomaly_active:
            self.anomaly_severity = severity
            return None

        # Anomaly has recovered.
        if not is_anomaly and self.anomaly_active:
            previous_severity = self.anomaly_severity

            self.anomaly_active = False
            self.anomaly_severity = "INFO"

            return {
                "type": "QUALITY_ANOMALY_RECOVERY",
                "severity": "RECOVERY",
                "current_quality": current_quality,
                "baseline_quality": baseline_quality,
                "deviation": round(deviation, 2),
                "message": "Quality anomaly has recovered",
                "timestamp": timestamp,
                "previous_severity": previous_severity,
            }

        # No anomaly and no previous anomaly.
        return None