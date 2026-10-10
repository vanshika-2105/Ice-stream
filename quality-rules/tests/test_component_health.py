from datetime import datetime, timezone

from component_health import (
    ComponentHealth,
    COMPONENT_HEALTHY,
    COMPONENT_DEGRADED,
    COMPONENT_FAILED,
    COMPONENT_RECOVERING,
    COMPONENT_UNKNOWN,
)


def test_component_health_creation():
    checked_at = datetime.now(timezone.utc)

    health = ComponentHealth(
        component="kafka",
        status=COMPONENT_HEALTHY,
        message="Kafka is available",
        checked_at=checked_at,
    )

    assert health.component == "kafka"
    assert health.status == "HEALTHY"
    assert health.message == "Kafka is available"
    assert health.checked_at == checked_at


def test_component_health_states():
    states = {
        COMPONENT_HEALTHY,
        COMPONENT_DEGRADED,
        COMPONENT_FAILED,
        COMPONENT_RECOVERING,
        COMPONENT_UNKNOWN,
    }

    assert states == {
        "HEALTHY",
        "DEGRADED",
        "FAILED",
        "RECOVERING",
        "UNKNOWN",
    }
