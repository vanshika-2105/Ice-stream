import importlib

import pytest

import alert_server.config as config


def test_default_config(monkeypatch):
    monkeypatch.delenv("PORT", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    monkeypatch.delenv("DEBUG", raising=False)
    monkeypatch.delenv("ALLOWED_ORIGINS", raising=False)
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.delenv("ICEBERG_URL", raising=False)
    monkeypatch.delenv(
        "WEBSOCKET_UPDATE_INTERVAL_SECONDS",
        raising=False,
    )
    monkeypatch.delenv(
        "PERFORMANCE_HISTORY_SIZE",
        raising=False,
    )

    module = importlib.reload(config)

    assert module.HOST == "0.0.0.0"
    assert module.PORT == 8000
    assert module.ENVIRONMENT == "development"
    assert module.DEBUG is True
    assert module.ALLOWED_ORIGINS == [
        "http://localhost:5173"
    ]
    assert module.KAFKA_BOOTSTRAP_SERVERS == "localhost:9092"
    assert module.ICEBERG_URL == "http://localhost:8181"
    assert module.WEBSOCKET_UPDATE_INTERVAL_SECONDS == 1
    assert module.PERFORMANCE_HISTORY_SIZE == 100


def test_custom_config(monkeypatch):
    monkeypatch.setenv("PORT", "9000")
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("DEBUG", "false")
    monkeypatch.setenv(
        "ALLOWED_ORIGINS",
        "http://localhost:3000, http://localhost:5173",
    )
    monkeypatch.setenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    )
    monkeypatch.setenv(
        "ICEBERG_URL",
        "http://iceberg:8181",
    )
    monkeypatch.setenv(
        "WEBSOCKET_UPDATE_INTERVAL_SECONDS",
        "2",
    )
    monkeypatch.setenv(
        "PERFORMANCE_HISTORY_SIZE",
        "200",
    )

    module = importlib.reload(config)

    assert module.PORT == 9000
    assert module.ENVIRONMENT == "production"
    assert module.DEBUG is False
    assert module.ALLOWED_ORIGINS == [
        "http://localhost:3000",
        "http://localhost:5173",
    ]
    assert module.KAFKA_BOOTSTRAP_SERVERS == "kafka:9092"
    assert module.ICEBERG_URL == "http://iceberg:8181"
    assert module.WEBSOCKET_UPDATE_INTERVAL_SECONDS == 2
    assert module.PERFORMANCE_HISTORY_SIZE == 200


def test_invalid_integer_config(monkeypatch):
    monkeypatch.setenv("PORT", "not-a-number")

    with pytest.raises(ValueError, match="PORT"):
        importlib.reload(config)


def test_invalid_boolean_config(monkeypatch):
    monkeypatch.setenv("DEBUG", "maybe")

    with pytest.raises(ValueError, match="DEBUG"):
        importlib.reload(config)