from alert_server.system_alerts import SystemAlertEngine


def test_system_alert_on_component_failure():
    engine = SystemAlertEngine()

    alert = engine.update_component(
        "kafka",
        "DOWN",
        "Kafka connection unavailable",
    )

    assert alert is not None

    data = alert.to_dict()

    assert data["type"] == "SYSTEM_ALERT"
    assert data["severity"] == "CRITICAL"
    assert data["component"] == "kafka"
    assert data["message"] == "Kafka connection unavailable"
    assert "timestamp" in data


def test_system_recovery_on_component_restore():
    engine = SystemAlertEngine()

    engine.update_component("kafka", "DOWN")

    alert = engine.update_component(
        "kafka",
        "UP",
        "Kafka connection restored",
    )

    assert alert is not None

    data = alert.to_dict()

    assert data["type"] == "SYSTEM_RECOVERY"
    assert data["component"] == "kafka"
    assert data["message"] == "Kafka connection restored"
    assert "timestamp" in data


def test_duplicate_down_does_not_create_duplicate_alert():
    engine = SystemAlertEngine()

    first = engine.update_component("kafka", "DOWN")
    second = engine.update_component("kafka", "DOWN")

    assert first is not None
    assert second is None

    assert len(engine.get_alert_history()) == 1


def test_duplicate_up_does_not_create_duplicate_recovery():
    engine = SystemAlertEngine()

    engine.update_component("kafka", "DOWN")

    first = engine.update_component("kafka", "UP")
    second = engine.update_component("kafka", "UP")

    assert first is not None
    assert second is None

    assert len(engine.get_alert_history()) == 2


def test_system_status_degraded_when_component_down():
    engine = SystemAlertEngine()

    engine.update_component("kafka", "DOWN")
    engine.update_component("flink", "UP")
    engine.update_component("iceberg", "UP")
    engine.update_component("alert_server", "UP")

    status = engine.get_system_status()

    assert status["system_status"] == "DEGRADED"
    assert status["components"]["kafka"] == "DOWN"
    assert status["components"]["flink"] == "UP"


def test_system_status_healthy_when_components_up():
    engine = SystemAlertEngine()

    engine.update_component("kafka", "UP")
    engine.update_component("flink", "UP")
    engine.update_component("iceberg", "UP")
    engine.update_component("alert_server", "UP")

    status = engine.get_system_status()

    assert status["system_status"] == "HEALTHY"