from fastapi.testclient import TestClient

from alert_server.main import app

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")

    assert response.status_code == 200

    data = response.json()

    assert data["status"] == "ok"
    assert data["service"] == "alert-server"
    assert "timestamp" in data


def test_quality_status_endpoint():
    response = client.get("/quality/status")

    assert response.status_code == 200

    data = response.json()

    assert "status" in data
    assert "quality_score" in data
    assert "total_events" in data
    assert "valid_events" in data
    assert "invalid_events" in data
    assert "invalid_event_rate" in data
    assert "trend" in data
    assert data["trend"] in {"UP", "DOWN", "STABLE"}


def test_alert_history_endpoint():
    response = client.get("/alerts")

    assert response.status_code == 200

    data = response.json()

    assert "alerts" in data
    assert isinstance(data["alerts"], list)
def test_quality_trend_down():
    from alert_server.main import quality_history, QualityMetricSnapshot
    from datetime import datetime, timezone

    quality_history.clear()

    quality_history.append(
        QualityMetricSnapshot(
            timestamp=datetime.now(timezone.utc),
            total_events=100,
            valid_events=95,
            invalid_events=5,
            quality_score=95.0,
            invalid_event_rate=5.0,
        )
    )

    quality_history.append(
        QualityMetricSnapshot(
            timestamp=datetime.now(timezone.utc),
            total_events=100,
            valid_events=93,
            invalid_events=7,
            quality_score=93.0,
            invalid_event_rate=7.0,
        )
    )

    response = client.get("/quality/status")

    assert response.status_code == 200
    assert response.json()["trend"] == "DOWN"


def test_quality_trend_up():
    from alert_server.main import quality_history, QualityMetricSnapshot
    from datetime import datetime, timezone

    quality_history.clear()

    quality_history.append(
        QualityMetricSnapshot(
            timestamp=datetime.now(timezone.utc),
            total_events=100,
            valid_events=90,
            invalid_events=10,
            quality_score=90.0,
            invalid_event_rate=10.0,
        )
    )

    quality_history.append(
        QualityMetricSnapshot(
            timestamp=datetime.now(timezone.utc),
            total_events=100,
            valid_events=96,
            invalid_events=4,
            quality_score=96.0,
            invalid_event_rate=4.0,
        )
    )

    response = client.get("/quality/status")

    assert response.status_code == 200
    assert response.json()["trend"] == "UP"


def test_quality_trend_stable():
    from alert_server.main import quality_history, QualityMetricSnapshot
    from datetime import datetime, timezone

    quality_history.clear()

    quality_history.append(
        QualityMetricSnapshot(
            timestamp=datetime.now(timezone.utc),
            total_events=100,
            valid_events=90,
            invalid_events=10,
            quality_score=90.0,
            invalid_event_rate=10.0,
        )
    )

    quality_history.append(
        QualityMetricSnapshot(
            timestamp=datetime.now(timezone.utc),
            total_events=100,
            valid_events=90,
            invalid_events=10,
            quality_score=90.0,
            invalid_event_rate=10.0,
        )

    )

    response = client.get("/quality/status")

    assert response.status_code == 200
    assert response.json()["trend"] == "STABLE"
def test_quality_history_empty():
    from alert_server.main import quality_history

    quality_history.clear()

    response = client.get("/quality/history")

    assert response.status_code == 200

    data = response.json()

    assert "history" in data
    assert isinstance(data["history"], list)
    assert data["history"] == []


def test_quality_history_returns_snapshots():
    from alert_server.main import quality_history, QualityMetricSnapshot
    from datetime import datetime, timezone

    quality_history.clear()

    quality_history.append(
        QualityMetricSnapshot(
            timestamp=datetime.now(timezone.utc),
            total_events=100,
            valid_events=95,
            invalid_events=5,
            quality_score=95.0,
            invalid_event_rate=5.0,
        )
    )

    response = client.get("/quality/history")

    assert response.status_code == 200

    data = response.json()

    assert len(data["history"]) == 1

    snapshot = data["history"][0]

    assert "timestamp" in snapshot
    assert snapshot["total_events"] == 100
    assert snapshot["valid_events"] == 95
    assert snapshot["invalid_events"] == 5
    assert snapshot["quality_score"] == 95.0
    assert snapshot["invalid_event_rate"] == 5.0


def test_quality_history_limit():
    from alert_server.main import quality_history, QualityMetricSnapshot
    from datetime import datetime, timezone

    quality_history.clear()

    for i in range(10):
        quality_history.append(
            QualityMetricSnapshot(
                timestamp=datetime.now(timezone.utc),
                total_events=i + 1,
                valid_events=i + 1,
                invalid_events=0,
                quality_score=100.0,
                invalid_event_rate=0.0,
            )
        )

    response = client.get("/quality/history?limit=5")

    assert response.status_code == 200

    data = response.json()

    assert len(data["history"]) == 5

    # Verify newest five snapshots are returned.
    assert data["history"][0]["total_events"] == 6
    assert data["history"][-1]["total_events"] == 10


def test_quality_history_invalid_limit():
    from alert_server.main import quality_history

    quality_history.clear()

    response = client.get("/quality/history?limit=1000")

    assert response.status_code == 400
def test_quality_anomaly_endpoint_with_insufficient_history():
    from alert_server.main import quality_history, quality_metrics

    quality_history.clear()

    quality_metrics.total_events = 0
    quality_metrics.valid_events = 0
    quality_metrics.invalid_events = 0
    quality_metrics.error_counts.clear()

    response = client.get("/quality/anomaly")

    assert response.status_code == 200

    data = response.json()

    assert data["is_anomaly"] is False
    assert data["current_quality"] == 0.0
    assert data["baseline_quality"] == 0.0
    assert data["deviation"] == 0.0
    assert data["reason"] == "Insufficient historical data"


def test_quality_anomaly_endpoint_detects_drop():
    from alert_server.main import quality_history, quality_metrics
    from models import QualityMetricSnapshot
    from datetime import datetime, timezone

    quality_history.clear()

    quality_history.extend([
        QualityMetricSnapshot(
            timestamp=datetime.now(timezone.utc),
            total_events=100,
            valid_events=98,
            invalid_events=2,
            quality_score=98.0,
            invalid_event_rate=2.0,
        ),
        QualityMetricSnapshot(
            timestamp=datetime.now(timezone.utc),
            total_events=200,
            valid_events=194,
            invalid_events=6,
            quality_score=97.0,
            invalid_event_rate=3.0,
        ),
        QualityMetricSnapshot(
            timestamp=datetime.now(timezone.utc),
            total_events=300,
            valid_events=294,
            invalid_events=6,
            quality_score=98.0,
            invalid_event_rate=2.0,
        ),
        QualityMetricSnapshot(
            timestamp=datetime.now(timezone.utc),
            total_events=400,
            valid_events=388,
            invalid_events=12,
            quality_score=97.0,
            invalid_event_rate=3.0,
        ),
        QualityMetricSnapshot(
            timestamp=datetime.now(timezone.utc),
            total_events=500,
            valid_events=480,
            invalid_events=20,
            quality_score=96.0,
            invalid_event_rate=4.0,
        ),
        QualityMetricSnapshot(
            timestamp=datetime.now(timezone.utc),
            total_events=550,
            valid_events=522,
            invalid_events=28,
            quality_score=90.0,
            invalid_event_rate=5.09,
        ),
    ])

    quality_metrics.total_events = 600
    quality_metrics.valid_events = 510
    quality_metrics.invalid_events = 90

    response = client.get("/quality/anomaly")

    assert response.status_code == 200

    data = response.json()

    assert data["is_anomaly"] is True
    assert data["current_quality"] == 85.0
    assert data["baseline_quality"] == 97.2
    assert data["deviation"] == 12.2
    assert (
        data["reason"]
        == "Quality dropped significantly below historical baseline"
    )
def test_websocket_receives_quality_anomaly():
    from alert_server.main import quality_history, quality_metrics

    quality_history.clear()

    quality_metrics.total_events = 0
    quality_metrics.valid_events = 0
    quality_metrics.invalid_events = 0
    quality_metrics.error_counts.clear()

    # Build a healthy historical baseline.
    for index in range(5):
        response = client.post(
            "/events",
            json={
                "event_id": f"ws-evt-{index}",
                "event_type": "checkout",
                "timestamp": f"2026-09-10T12:0{index}:00Z",
                "order_id": f"ws-order-{index}",
                "customer_id": f"ws-customer-{index}",
                "product_id": f"ws-product-{index}",
                "quantity": 1,
                "amount": 100,
                "currency": "INR",
            },
        )

        assert response.status_code == 200
        assert response.json()["valid"] is True

    with client.websocket_connect("/ws/alerts") as websocket:
        # Trigger a quality drop.
        response = client.post(
            "/events",
            json={
                "event_id": "ws-anomaly-evt",
                "event_type": "checkout",
                "timestamp": "2026-09-10T12:05:00Z",
                "order_id": "ws-anomaly-order",
                "customer_id": "ws-anomaly-customer",
                "product_id": "ws-anomaly-product",
                "quantity": 1,
                "amount": 100,
                "currency": "XYZ",
            },
        )

        assert response.status_code == 200
        assert response.json()["valid"] is False

        messages = [
            websocket.receive_json(),
            websocket.receive_json(),
        ]

        message_types = {
            message["type"]
            for message in messages
        }

        assert "QUALITY_METRICS" in message_types
        assert "QUALITY_ANOMALY" in message_types

        anomaly_message = next(
            message
            for message in messages
            if message["type"] == "QUALITY_ANOMALY"
        )

        assert anomaly_message["is_anomaly"] is True
        assert anomaly_message["current_quality"] == 83.33
        assert anomaly_message["baseline_quality"] == 100.0
        assert anomaly_message["deviation"] == 16.67
        assert (
            anomaly_message["reason"]
            == "Quality dropped significantly below historical baseline"
        )