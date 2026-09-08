from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class SystemAlert:
    type: str
    severity: str
    component: str
    message: str
    timestamp: str

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "severity": self.severity,
            "component": self.component,
            "message": self.message,
            "timestamp": self.timestamp,
        }


class SystemAlertEngine:
    def __init__(self):
        self.component_status = {}
        self.alert_history = []

    def update_component(
        self,
        component: str,
        status: str,
        message: str | None = None,
    ):
        """
        Update infrastructure component status.

        DOWN -> SYSTEM_ALERT
        DOWN -> UP -> SYSTEM_RECOVERY
        """

        previous_status = self.component_status.get(component)

        self.component_status[component] = status

        if status == "DOWN" and previous_status != "DOWN":
            alert = SystemAlert(
                type="SYSTEM_ALERT",
                severity="CRITICAL",
                component=component,
                message=message or f"{component} connection unavailable",
                timestamp=datetime.now(timezone.utc).isoformat(),
            )

            self.alert_history.append(alert.to_dict())
            return alert

        if (
            status == "UP"
            and previous_status == "DOWN"
        ):
            alert = SystemAlert(
                type="SYSTEM_RECOVERY",
                severity="INFO",
                component=component,
                message=message or f"{component} connection restored",
                timestamp=datetime.now(timezone.utc).isoformat(),
            )

            self.alert_history.append(alert.to_dict())
            return alert

        return None

    def get_system_status(self) -> dict:
        """
        Return overall infrastructure status.
        """

        components = dict(self.component_status)

        if any(status == "DOWN" for status in components.values()):
            system_status = "DEGRADED"
        else:
            system_status = "HEALTHY"

        return {
            "system_status": system_status,
            "components": components,
        }

    def get_alert_history(self) -> list:
        return list(self.alert_history)