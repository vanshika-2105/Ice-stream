from datetime import datetime, timedelta, timezone

from health_checks import (
    check_kafka,
    check_producer,
    check_flink,
    check_backend,
    check_websocket,
    check_iceberg,
)


def test_kafka_healthy():
    result = check_kafka("UP")

    assert result.component == "kafka"
    assert result.status == "HEALTHY"


def test_kafka_failed():
    result = check_kafka("DOWN")

    assert result.component == "kafka"
    assert result.status == "FAILED"


def test_kafka_unknown():
    result = check_kafka()

    assert result.status == "UNKNOWN"


def test_producer_healthy():
    last_event = datetime.now(timezone.utc)

    result = check_producer(last_event)

    assert result.component == "producer"
    assert result.status == "HEALTHY"


def test_producer_degraded_when_stale():
    last_event = datetime.now(timezone.utc) - timedelta(seconds=20)

    result = check_producer(last_event)

    assert result.component == "producer"
    assert result.status == "DEGRADED"


def test_producer_unknown_without_events():
    result = check_producer(None)

    assert result.component == "producer"
    assert result.status == "UNKNOWN"


def test_flink_healthy():
    result = check_flink(True)

    assert result.component == "flink"
    assert result.status == "HEALTHY"


def test_flink_degraded():
    result = check_flink(False)

    assert result.component == "flink"
    assert result.status == "DEGRADED"


def test_flink_unknown():
    result = check_flink()

    assert result.status == "UNKNOWN"


def test_backend_healthy():
    result = check_backend()

    assert result.component == "backend"
    assert result.status == "HEALTHY"


def test_websocket_healthy():
    result = check_websocket("CONNECTED")

    assert result.component == "websocket"
    assert result.status == "HEALTHY"


def test_websocket_degraded():
    result = check_websocket("DISCONNECTED")

    assert result.component == "websocket"
    assert result.status == "DEGRADED"


def test_iceberg_healthy():
    result = check_iceberg("UP")

    assert result.component == "iceberg"
    assert result.status == "HEALTHY"


def test_iceberg_failed():
    result = check_iceberg("DOWN")

    assert result.component == "iceberg"
    assert result.status == "FAILED"


def test_iceberg_unknown():
    result = check_iceberg()

    assert result.status == "UNKNOWN"
