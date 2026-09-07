from fastapi.testclient import TestClient

from alert_server.main import app


client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")

    assert response.status_code == 200

    data = response.json()

    assert data["status"] == "healthy"
    assert data["service"] == "alert-server"


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


def test_alert_history_endpoint():
    response = client.get("/alerts")

    assert response.status_code == 200

    data = response.json()

    assert "alerts" in data
    assert isinstance(data["alerts"], list)