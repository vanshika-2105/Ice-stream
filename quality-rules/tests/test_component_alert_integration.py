from alert_server.system_alerts import SystemAlertEngine
from health_checks import check_kafka


def test_kafka_failure_creates_system_alert():
    engine = SystemAlertEngine()

    health = check_kafka("DOWN")

    assert health.status == "FAILED"

    alert = engine.update_component(
        health.component,
        "DOWN",
        health.message,
    )

    assert alert is not None

    data = alert.to_dict()

    assert data["type"] == "SYSTEM_ALERT"
    assert data["component"] == "kafka"
    assert data["status"] == "FAILED"


def test_kafka_failure_is_deduplicated():
    engine = SystemAlertEngine()

    health = check_kafka("DOWN")

    first = engine.update_component(
        health.component,
        "DOWN",
        health.message,
    )

    second = engine.update_component(
        health.component,
        "DOWN",
        health.message,
    )

    assert first is not None
    assert second is None
    assert len(engine.get_alert_history()) == 1


def test_kafka_recovery_creates_system_recovery():
    engine = SystemAlertEngine()

    failed = check_kafka("DOWN")

    engine.update_component(
        failed.component,
        "DOWN",
        failed.message,
    )

    healthy = check_kafka("UP")

    alert = engine.update_component(
        healthy.component,
        "UP",
        healthy.message,
    )

    assert alert is not None

    data = alert.to_dict()

    assert data["type"] == "SYSTEM_RECOVERY"
    assert data["component"] == "kafka"
    assert data["status"] == "RECOVERED"
