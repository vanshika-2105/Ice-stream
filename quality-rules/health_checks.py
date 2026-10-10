from datetime import datetime, timezone

from component_health import (
    ComponentHealth,
    COMPONENT_HEALTHY,
    COMPONENT_DEGRADED,
    COMPONENT_FAILED,
    COMPONENT_UNKNOWN,
)


PRODUCER_STALE_THRESHOLD_SECONDS = 10.0


def _now() -> datetime:
    return datetime.now(timezone.utc)


def check_kafka(status: str | None = None) -> ComponentHealth:
    """Evaluate Kafka health from an existing infrastructure signal."""

    checked_at = _now()

    if status == "UP":
        return ComponentHealth(
            component="kafka",
            status=COMPONENT_HEALTHY,
            message="Kafka is available",
            checked_at=checked_at,
        )

    if status == "DOWN":
        return ComponentHealth(
            component="kafka",
            status=COMPONENT_FAILED,
            message="Kafka is unavailable",
            checked_at=checked_at,
        )

    return ComponentHealth(
        component="kafka",
        status=COMPONENT_UNKNOWN,
        message="Kafka health signal is unavailable",
        checked_at=checked_at,
    )


def check_producer(last_event_time: datetime | None) -> ComponentHealth:
    """Evaluate producer health from the latest event timestamp."""

    checked_at = _now()

    if last_event_time is None:
        return ComponentHealth(
            component="producer",
            status=COMPONENT_UNKNOWN,
            message="No producer activity has been observed",
            checked_at=checked_at,
        )

    if last_event_time.tzinfo is None:
        last_event_time = last_event_time.replace(tzinfo=timezone.utc)

    age_seconds = (
        checked_at - last_event_time
    ).total_seconds()

    if age_seconds <= PRODUCER_STALE_THRESHOLD_SECONDS:
        return ComponentHealth(
            component="producer",
            status=COMPONENT_HEALTHY,
            message="Recent producer activity detected",
            checked_at=checked_at,
        )

    return ComponentHealth(
        component="producer",
        status=COMPONENT_DEGRADED,
        message="No recent producer activity detected",
        checked_at=checked_at,
    )


def check_flink(processing_active: bool | None = None) -> ComponentHealth:
    """Evaluate Flink health from an existing processing signal."""

    checked_at = _now()

    if processing_active is True:
        return ComponentHealth(
            component="flink",
            status=COMPONENT_HEALTHY,
            message="Recent Flink processing activity detected",
            checked_at=checked_at,
        )

    if processing_active is False:
        return ComponentHealth(
            component="flink",
            status=COMPONENT_DEGRADED,
            message="No recent Flink processing activity detected",
            checked_at=checked_at,
        )

    return ComponentHealth(
        component="flink",
        status=COMPONENT_UNKNOWN,
        message="Flink health signal is unavailable",
        checked_at=checked_at,
    )


def check_backend() -> ComponentHealth:
    """The fact that this function executes means the backend is reachable."""

    return ComponentHealth(
        component="backend",
        status=COMPONENT_HEALTHY,
        message="Backend is responding",
        checked_at=_now(),
    )


def check_websocket(connection_state: str | None) -> ComponentHealth:
    """Evaluate WebSocket health from the AlertManager connection state."""

    checked_at = _now()

    if connection_state == "CONNECTED":
        return ComponentHealth(
            component="websocket",
            status=COMPONENT_HEALTHY,
            message="WebSocket client connected",
            checked_at=checked_at,
        )

    if connection_state == "DISCONNECTED":
        return ComponentHealth(
            component="websocket",
            status=COMPONENT_DEGRADED,
            message="No WebSocket clients connected",
            checked_at=checked_at,
        )

    return ComponentHealth(
        component="websocket",
        status=COMPONENT_UNKNOWN,
        message="WebSocket health signal is unavailable",
        checked_at=checked_at,
    )


def check_iceberg(status: str | None = None) -> ComponentHealth:
    """Evaluate Iceberg health from an existing infrastructure signal."""

    checked_at = _now()

    if status == "UP":
        return ComponentHealth(
            component="iceberg",
            status=COMPONENT_HEALTHY,
            message="Iceberg is available",
            checked_at=checked_at,
        )

    if status == "DOWN":
        return ComponentHealth(
            component="iceberg",
            status=COMPONENT_FAILED,
            message="Iceberg is unavailable",
            checked_at=checked_at,
        )

    return ComponentHealth(
        component="iceberg",
        status=COMPONENT_UNKNOWN,
        message="Iceberg health signal is unavailable",
        checked_at=checked_at,
    )
