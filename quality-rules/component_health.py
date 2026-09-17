from dataclasses import dataclass
from datetime import datetime


COMPONENT_HEALTHY = "HEALTHY"
COMPONENT_DEGRADED = "DEGRADED"
COMPONENT_FAILED = "FAILED"
COMPONENT_RECOVERING = "RECOVERING"
COMPONENT_UNKNOWN = "UNKNOWN"


@dataclass
class ComponentHealth:
    """Represent the current health of a pipeline component."""

    component: str
    status: str
    message: str
    checked_at: datetime
