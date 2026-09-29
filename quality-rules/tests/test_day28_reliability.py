import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


# Add the project root so alert_server can be imported.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from alert_server.main import app


client = TestClient(app)


def test_unknown_endpoint_returns_404():
    response = client.get("/does-not-exist")

    assert response.status_code == 404
    assert response.json()["detail"] == "Not Found"


def test_quality_history_rejects_invalid_limit():
    response = client.get("/quality/history?limit=0")

    assert response.status_code == 400
    assert "between 1 and 100" in response.json()["detail"]


def test_quality_history_rejects_limit_above_maximum():
    response = client.get("/quality/history?limit=101")

    assert response.status_code == 400
    assert "between 1 and 100" in response.json()["detail"]


def test_unknown_alert_acknowledge_returns_404():
    response = client.post(
        "/alerts/does-not-exist/acknowledge"
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_invalid_event_returns_controlled_validation_response():
    response = client.post(
        "/events",
        json={},
    )

    assert response.status_code == 200

    data = response.json()

    assert data["valid"] is False
    assert len(data["errors"]) > 0


def test_invalid_json_returns_422():
    response = client.post(
        "/events",
        content="{invalid-json",
        headers={"Content-Type": "application/json"},
    )

    assert response.status_code == 422


def test_health_endpoint():
    response = client.get("/health")

    assert response.status_code == 200

    data = response.json()

    assert data["status"] == "ok"
    assert data["service"] == "alert-server"