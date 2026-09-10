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