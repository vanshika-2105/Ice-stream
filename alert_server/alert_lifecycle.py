from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


ACTIVE = "ACTIVE"
ACKNOWLEDGED = "ACKNOWLEDGED"
RESOLVED = "RESOLVED"

INFO = "INFO"
WARNING = "WARNING"
CRITICAL = "CRITICAL"


@dataclass
class LifecycleAlert:
    """Represent an alert through its complete lifecycle."""

    alert_id: str
    alert_type: str
    severity: str
    component: str
    message: str
    created_at: str
    status: str = ACTIVE
    resolved_at: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert the lifecycle alert to a JSON-friendly dictionary."""
        return {
            "alert_id": self.alert_id,
            "alert_type": self.alert_type,
            "severity": self.severity,
            "component": self.component,
            "message": self.message,
            "created_at": self.created_at,
            "status": self.status,
            "resolved_at": self.resolved_at,
        }


class AlertLifecycleManager:
    """
    Manage alert lifecycle state.

    Lifecycle:

        condition detected
              |
            ACTIVE
              |
        acknowledge()
              |
        ACKNOWLEDGED
              |
       resolve/recovery
              |
          RESOLVED
    """

    def __init__(self, max_history: int = 100):
        self.max_history = max_history
        self.active_alerts: dict[str, LifecycleAlert] = {}
        self.alert_history: list[LifecycleAlert] = []
        self._next_alert_number = 1

    def _generate_alert_id(self) -> str:
        """Generate a unique human-readable alert ID."""
        alert_id = f"ALT-{self._next_alert_number:06d}"
        self._next_alert_number += 1
        return alert_id

    @staticmethod
    def _timestamp() -> str:
        """Return the current UTC timestamp."""
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _condition_key(
        alert_type: str,
        component: str,
    ) -> str:
        """
        Build the deduplication key for an ongoing condition.
        """
        return f"{alert_type}:{component}"

    def create_alert(
        self,
        alert_type: str,
        severity: str,
        component: str,
        message: str,
    ) -> tuple[LifecycleAlert, bool]:
        """
        Create an ACTIVE alert.

        Returns:
            (alert, created)

        created=True means a new alert was created.
        created=False means an existing active alert was reused.
        """

        condition_key = self._condition_key(
            alert_type,
            component,
        )

        # Preserve deduplication for ongoing conditions.
        for alert in self.active_alerts.values():
            if (
                self._condition_key(
                    alert.alert_type,
                    alert.component,
                )
                == condition_key
            ):
                # Update the ongoing alert without creating
                # another active alert.
                alert.severity = severity
                alert.message = message
                return alert, False

        alert = LifecycleAlert(
            alert_id=self._generate_alert_id(),
            alert_type=alert_type,
            severity=severity,
            component=component,
            message=message,
            created_at=self._timestamp(),
            status=ACTIVE,
        )

        self.active_alerts[alert.alert_id] = alert

        return alert, True

    def acknowledge(self, alert_id: str) -> Optional[LifecycleAlert]:
        """
        Acknowledge an active alert.

        Returns None when the alert does not exist or is already resolved.
        """

        alert = self.active_alerts.get(alert_id)

        if alert is None:
            return None

        if alert.status == ACTIVE:
            alert.status = ACKNOWLEDGED

        return alert

    def resolve(self, alert_id: str) -> Optional[LifecycleAlert]:
        """
        Resolve an active or acknowledged alert.
        """

        alert = self.active_alerts.pop(alert_id, None)

        if alert is None:
            return None

        alert.status = RESOLVED
        alert.resolved_at = self._timestamp()

        self.alert_history.append(alert)

        if len(self.alert_history) > self.max_history:
            self.alert_history.pop(0)

        return alert

    def resolve_condition(
        self,
        alert_type: str,
        component: str,
    ) -> Optional[LifecycleAlert]:
        """
        Resolve the active alert belonging to a condition.
        """

        condition_key = self._condition_key(
            alert_type,
            component,
        )

        for alert_id, alert in list(self.active_alerts.items()):
            current_key = self._condition_key(
                alert.alert_type,
                alert.component,
            )

            if current_key == condition_key:
                return self.resolve(alert_id)

        return None

    def get_active(self) -> list[dict]:
        """Return currently active and acknowledged alerts."""
        return [
            alert.to_dict()
            for alert in self.active_alerts.values()
        ]

    def get_history(self) -> list[dict]:
        """Return resolved alert history."""
        return [
            alert.to_dict()
            for alert in self.alert_history
        ]

    def get_alert(self, alert_id: str) -> Optional[LifecycleAlert]:
        """Find an active alert by ID."""
        return self.active_alerts.get(alert_id)

    def summary(self) -> dict:
        """Return a compact alert lifecycle summary."""

        active = list(self.active_alerts.values())

        critical_count = sum(
            alert.severity == CRITICAL
            for alert in active
        )

        warning_count = sum(
            alert.severity == WARNING
            for alert in active
        )

        return {
            "active_count": len(active),
            "critical_count": critical_count,
            "warning_count": warning_count,
            "resolved_count": len(self.alert_history),
            "alerts": [
                alert.to_dict()
                for alert in active
            ],
        }