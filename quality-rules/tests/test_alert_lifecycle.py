
import sys
from pathlib import Path

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[2])
)
from alert_server.alert_lifecycle import (
    ACTIVE,
    ACKNOWLEDGED,
    RESOLVED,
    AlertLifecycleManager,
)


def create_manager():
    return AlertLifecycleManager()


def test_alert_creation_starts_active():
    manager = create_manager()

    alert, created = manager.create_alert(
        alert_type="QUALITY_ALERT",
        severity="CRITICAL",
        component="quality",
        message="Data quality dropped below critical threshold",
    )

    assert created is True
    assert alert.alert_id == "ALT-000001"
    assert alert.status == ACTIVE
    assert alert.severity == "CRITICAL"
    assert alert.component == "quality"
    assert alert.resolved_at is None


def test_alert_acknowledge():
    manager = create_manager()

    alert, _ = manager.create_alert(
        alert_type="QUALITY_ALERT",
        severity="WARNING",
        component="quality",
        message="Data quality entered warning range",
    )

    acknowledged = manager.acknowledge(alert.alert_id)

    assert acknowledged is not None
    assert acknowledged.status == ACKNOWLEDGED

    active = manager.get_active()

    assert len(active) == 1
    assert active[0]["status"] == ACKNOWLEDGED


def test_alert_resolve():
    manager = create_manager()

    alert, _ = manager.create_alert(
        alert_type="QUALITY_ALERT",
        severity="CRITICAL",
        component="quality",
        message="Critical quality issue",
    )

    resolved = manager.resolve(alert.alert_id)

    assert resolved is not None
    assert resolved.status == RESOLVED
    assert resolved.resolved_at is not None

    assert manager.get_active() == []

    history = manager.get_history()

    assert len(history) == 1
    assert history[0]["alert_id"] == alert.alert_id
    assert history[0]["status"] == RESOLVED


def test_alert_history():
    manager = create_manager()

    first, _ = manager.create_alert(
        alert_type="QUALITY_ALERT",
        severity="WARNING",
        component="quality",
        message="Quality warning",
    )

    second, _ = manager.create_alert(
        alert_type="SYSTEM_ALERT",
        severity="CRITICAL",
        component="kafka",
        message="Kafka unavailable",
    )

    manager.resolve(first.alert_id)
    manager.resolve(second.alert_id)

    history = manager.get_history()

    assert len(history) == 2
    assert history[0]["alert_id"] == first.alert_id
    assert history[1]["alert_id"] == second.alert_id


def test_active_alerts():
    manager = create_manager()

    first, _ = manager.create_alert(
        alert_type="QUALITY_ALERT",
        severity="WARNING",
        component="quality",
        message="Quality warning",
    )

    second, _ = manager.create_alert(
        alert_type="SYSTEM_ALERT",
        severity="CRITICAL",
        component="kafka",
        message="Kafka unavailable",
    )

    manager.resolve(first.alert_id)

    active = manager.get_active()

    assert len(active) == 1
    assert active[0]["alert_id"] == second.alert_id
    assert active[0]["status"] == ACTIVE


def test_unknown_alert_id():
    manager = create_manager()

    assert manager.acknowledge("ALT-999999") is None
    assert manager.resolve("ALT-999999") is None


def test_alert_deduplication():
    manager = create_manager()

    first, created_first = manager.create_alert(
        alert_type="QUALITY_ALERT",
        severity="WARNING",
        component="quality",
        message="Quality warning",
    )

    second, created_second = manager.create_alert(
        alert_type="QUALITY_ALERT",
        severity="WARNING",
        component="quality",
        message="Quality warning continues",
    )

    assert created_first is True
    assert created_second is False

    assert first.alert_id == second.alert_id
    assert len(manager.get_active()) == 1

    # Existing alert can be updated without creating a duplicate.
    assert manager.get_active()[0]["message"] == "Quality warning continues"


def test_recovery_resolves_active_alert():
    manager = create_manager()

    alert, _ = manager.create_alert(
        alert_type="SYSTEM_ALERT",
        severity="CRITICAL",
        component="kafka",
        message="Kafka connection unavailable",
    )

    resolved = manager.resolve_condition(
        alert_type="SYSTEM_ALERT",
        component="kafka",
    )

    assert resolved is not None
    assert resolved.alert_id == alert.alert_id
    assert resolved.status == RESOLVED
    assert manager.get_active() == []
    assert len(manager.get_history()) == 1


def test_new_alert_after_resolution_gets_new_id():
    manager = create_manager()

    first, _ = manager.create_alert(
        alert_type="SYSTEM_ALERT",
        severity="CRITICAL",
        component="kafka",
        message="Kafka unavailable",
    )

    manager.resolve_condition(
        alert_type="SYSTEM_ALERT",
        component="kafka",
    )

    second, created = manager.create_alert(
        alert_type="SYSTEM_ALERT",
        severity="CRITICAL",
        component="kafka",
        message="Kafka unavailable again",
    )

    assert created is True
    assert first.alert_id != second.alert_id
    assert second.alert_id == "ALT-000002"


def test_summary():
    manager = create_manager()

    warning, _ = manager.create_alert(
        alert_type="QUALITY_ALERT",
        severity="WARNING",
        component="quality",
        message="Quality warning",
    )

    critical, _ = manager.create_alert(
        alert_type="SYSTEM_ALERT",
        severity="CRITICAL",
        component="kafka",
        message="Kafka unavailable",
    )

    manager.resolve(warning.alert_id)

    summary = manager.summary()

    assert summary["active_count"] == 1
    assert summary["critical_count"] == 1
    assert summary["warning_count"] == 0
    assert summary["resolved_count"] == 1
    assert len(summary["alerts"]) == 1
    assert summary["alerts"][0]["alert_id"] == critical.alert_id
def test_api_active_alerts_endpoint():
    from fastapi.testclient import TestClient
    from alert_server.main import app, alert_lifecycle

    alert_lifecycle.active_alerts.clear()
    alert_lifecycle.alert_history.clear()

    client = TestClient(app)

    alert, _ = alert_lifecycle.create_alert(
        alert_type="QUALITY_ALERT",
        severity="CRITICAL",
        component="quality",
        message="Test quality alert",
    )

    response = client.get("/alerts/active")

    assert response.status_code == 200

    data = response.json()

    assert len(data["alerts"]) == 1
    assert data["alerts"][0]["alert_id"] == alert.alert_id
    assert data["alerts"][0]["status"] == "ACTIVE"


def test_api_acknowledge_endpoint():
    from fastapi.testclient import TestClient
    from alert_server.main import app, alert_lifecycle

    alert_lifecycle.active_alerts.clear()
    alert_lifecycle.alert_history.clear()

    client = TestClient(app)

    alert, _ = alert_lifecycle.create_alert(
        alert_type="QUALITY_ALERT",
        severity="WARNING",
        component="quality",
        message="Test warning",
    )

    response = client.post(
        f"/alerts/{alert.alert_id}/acknowledge"
    )

    assert response.status_code == 200

    data = response.json()

    assert data["alert"]["alert_id"] == alert.alert_id
    assert data["alert"]["status"] == "ACKNOWLEDGED"


def test_api_acknowledge_unknown_alert():
    from fastapi.testclient import TestClient
    from alert_server.main import app, alert_lifecycle

    alert_lifecycle.active_alerts.clear()
    alert_lifecycle.alert_history.clear()

    client = TestClient(app)

    response = client.post(
        "/alerts/ALT-999999/acknowledge"
    )

    assert response.status_code == 404


def test_api_alert_history_endpoint():
    from fastapi.testclient import TestClient
    from alert_server.main import app, alert_lifecycle

    alert_lifecycle.active_alerts.clear()
    alert_lifecycle.alert_history.clear()

    client = TestClient(app)

    alert, _ = alert_lifecycle.create_alert(
        alert_type="SYSTEM_ALERT",
        severity="CRITICAL",
        component="kafka",
        message="Kafka unavailable",
    )

    alert_lifecycle.resolve(alert.alert_id)

    response = client.get("/alerts/history")

    assert response.status_code == 200

    data = response.json()

    assert len(data["alerts"]) == 1
    assert data["alerts"][0]["alert_id"] == alert.alert_id
    assert data["alerts"][0]["status"] == "RESOLVED"


def test_system_alert_recovery_lifecycle():
    from fastapi.testclient import TestClient
    from alert_server.main import (
        app,
        alert_lifecycle,
        system_alert_engine,
    )

    alert_lifecycle.active_alerts.clear()
    alert_lifecycle.alert_history.clear()
    system_alert_engine.component_status.clear()
    system_alert_engine.alert_history.clear()

    client = TestClient(app)

    down_response = client.post(
        "/system/components/kafka",
        json={
            "status": "DOWN",
            "message": "Kafka unavailable",
        },
    )

    assert down_response.status_code == 200
    assert down_response.json()["alert"]["type"] == "SYSTEM_ALERT"

    active = client.get("/alerts/active").json()

    assert len(active["alerts"]) == 1
    alert_id = active["alerts"][0]["alert_id"]
    assert active["alerts"][0]["status"] == "ACTIVE"

    up_response = client.post(
        "/system/components/kafka",
        json={
            "status": "UP",
            "message": "Kafka restored",
        },
    )

    assert up_response.status_code == 200
    assert up_response.json()["alert"]["type"] == "SYSTEM_RECOVERY"

    active_after_recovery = client.get(
        "/alerts/active"
    ).json()

    assert active_after_recovery["alerts"] == []

    history = client.get("/alerts/history").json()

    assert len(history["alerts"]) == 1
    assert history["alerts"][0]["alert_id"] == alert_id
    assert history["alerts"][0]["status"] == "RESOLVED"
def test_pipeline_degraded_creates_lifecycle_alert():
    from fastapi.testclient import TestClient
    from alert_server.main import app, alert_lifecycle
    import alert_server.main as main_module

    alert_lifecycle.active_alerts.clear()
    alert_lifecycle.alert_history.clear()

    main_module.pipeline_status = "HEALTHY"

    client = TestClient(app)

    # Force a pipeline alert through the lifecycle manager.
    alert, created = alert_lifecycle.create_alert(
        alert_type="PIPELINE_ALERT",
        severity="WARNING",
        component="pipeline",
        message="Pipeline performance is degraded.",
    )

    assert created is True
    assert alert.status == "ACTIVE"
    assert alert.severity == "WARNING"

    active = client.get("/alerts/active").json()

    assert len(active["alerts"]) == 1
    assert active["alerts"][0]["alert_type"] == "PIPELINE_ALERT"
    assert active["alerts"][0]["severity"] == "WARNING"


def test_pipeline_critical_updates_existing_alert():
    from alert_server.main import alert_lifecycle

    alert_lifecycle.active_alerts.clear()
    alert_lifecycle.alert_history.clear()

    first_alert, first_created = alert_lifecycle.create_alert(
        alert_type="PIPELINE_ALERT",
        severity="WARNING",
        component="pipeline",
        message="Pipeline performance is degraded.",
    )

    second_alert, second_created = alert_lifecycle.create_alert(
        alert_type="PIPELINE_ALERT",
        severity="CRITICAL",
        component="pipeline",
        message="Pipeline performance is critical.",
    )

    assert first_created is True
    assert second_created is False

    assert first_alert.alert_id == second_alert.alert_id
    assert second_alert.severity == "CRITICAL"
    assert second_alert.status == "ACTIVE"


def test_pipeline_recovery_resolves_lifecycle_alert():
    from alert_server.main import alert_lifecycle

    alert_lifecycle.active_alerts.clear()
    alert_lifecycle.alert_history.clear()

    alert, created = alert_lifecycle.create_alert(
        alert_type="PIPELINE_ALERT",
        severity="CRITICAL",
        component="pipeline",
        message="Pipeline performance is critical.",
    )

    assert created is True

    resolved = alert_lifecycle.resolve_condition(
        alert_type="PIPELINE_ALERT",
        component="pipeline",
    )

    assert resolved is not None
    assert resolved.alert_id == alert.alert_id
    assert resolved.status == "RESOLVED"
    assert resolved.resolved_at is not None

    assert alert_lifecycle.get_active() == []
    assert len(alert_lifecycle.get_history()) == 1
def test_anomaly_event_creates_lifecycle_alert():
    from fastapi.testclient import TestClient
    from alert_server.main import (
        app,
        alert_lifecycle,
        alert_engine,
        quality_history,
        quality_metrics,
        profile_event_type_counts,
        profile_currency_counts,
        pipeline_latencies_ms,
    )
    import alert_server.main as main_module

    # Reset shared application state.
    alert_lifecycle.active_alerts.clear()
    alert_lifecycle.alert_history.clear()

    alert_engine.current_status = "HEALTHY"
    alert_engine.anomaly_active = False
    alert_engine.anomaly_severity = "INFO"

    quality_history.clear()

    quality_metrics.total_events = 0
    quality_metrics.valid_events = 0
    quality_metrics.invalid_events = 0
    quality_metrics.error_counts.clear()

    profile_event_type_counts.clear()
    profile_currency_counts.clear()
    pipeline_latencies_ms.clear()

    main_module.pipeline_status = "HEALTHY"
    main_module.pipeline_start_time = None
    main_module.pipeline_elapsed_seconds = 0.0

    client = TestClient(app)

    # Build a healthy historical baseline.
    for index in range(5):
        response = client.post(
            "/events",
            json={
                "event_id": f"lifecycle-baseline-{index}",
                "event_type": "checkout",
                "timestamp": f"2026-09-10T12:0{index}:00Z",
                "order_id": f"lifecycle-order-{index}",
                "customer_id": f"lifecycle-customer-{index}",
                "product_id": f"lifecycle-product-{index}",
                "quantity": 1,
                "amount": 100,
                "currency": "INR",
            },
        )

        assert response.status_code == 200
        assert response.json()["valid"] is True

    # Create a large quality drop to trigger an anomaly.
    response = client.post(
        "/events",
        json={
            "event_id": "lifecycle-anomaly-event",
            "event_type": "checkout",
            "timestamp": "2026-09-10T12:05:00Z",
            "order_id": "lifecycle-anomaly-order",
            "customer_id": "lifecycle-anomaly-customer",
            "product_id": "lifecycle-anomaly-product",
            "quantity": 1,
            "amount": 100,
            "currency": "XYZ",
        },
    )

    assert response.status_code == 200
    assert response.json()["valid"] is False

    active_alerts = client.get("/alerts/active")

    assert active_alerts.status_code == 200

    lifecycle_alerts = active_alerts.json()["alerts"]

    anomaly_alert = next(
        alert
        for alert in lifecycle_alerts
        if alert["alert_type"] == "QUALITY_ANOMALY_ALERT"
    )

    assert anomaly_alert["status"] == "ACTIVE"
    assert anomaly_alert["component"] == "quality-anomaly"
    assert anomaly_alert["severity"] in {
        "WARNING",
        "CRITICAL",
    }