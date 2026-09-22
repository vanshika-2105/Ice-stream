from fastapi.testclient import TestClient

from alert_server.main import app

client = TestClient(app)
def test_quality_status_integration():
    response = client.get("/quality/status")

    assert response.status_code == 200

    data = response.json()

    expected_fields = {
        "status",
        "quality_score",
        "total_events",
        "valid_events",
        "invalid_events",
        "invalid_event_rate",
        "trend",
        "is_anomaly",
        "anomaly_severity",
    }

    assert expected_fields.issubset(data.keys())


def test_quality_history_integration():
    response = client.get("/quality/history")

    assert response.status_code == 200

    data = response.json()

    assert "history" in data
    assert isinstance(data["history"], list)


def test_quality_anomaly_integration():
    response = client.get("/quality/anomaly")

    assert response.status_code == 200

    data = response.json()

    expected_fields = {
        "is_anomaly",
        "severity",
        "current_quality",
        "baseline_quality",
        "deviation",
        "reason",
    }

    assert expected_fields.issubset(data.keys())


def test_quality_profile_integration():
    response = client.get("/quality/profile")

    assert response.status_code == 200

    data = response.json()

    expected_fields = {
        "total_events",
        "valid_events",
        "invalid_events",
        "error_counts",
        "error_percentages",
        "event_type_counts",
        "currency_counts",
        "top_error",
    }

    assert expected_fields.issubset(data.keys())

def test_health_integration():
    response = client.get("/health")

    assert response.status_code == 200

    data = response.json()

    assert data["status"] == "ok"
    assert data["service"] == "alert-server"
    assert "timestamp" in data


def test_live_observability_integration():
    response = client.get("/observability/live")

    assert response.status_code == 200

    data = response.json()

    expected_fields = {
        "status",
        "total_events",
        "valid_events",
        "invalid_events",
        "throughput_eps",
        "average_latency_ms",
        "dlq_rate",
        "quality_percentage",
        "last_event_time",
    }

    assert expected_fields.issubset(data.keys())


def test_observability_overview_integration():
    response = client.get("/observability/overview")

    assert response.status_code == 200

    data = response.json()

    expected_sections = {
        "quality",
        "anomaly",
        "profile",
        "pipeline",
        "system",
        "recovery",
        "overall_status",
    }

    assert expected_sections.issubset(data.keys())


def test_observability_overview_nested_structure():
    response = client.get("/observability/overview")

    assert response.status_code == 200

    data = response.json()

    assert "quality_score" in data["quality"]
    assert "status" in data["quality"]

    assert "is_anomaly" in data["anomaly"]
    assert "severity" in data["anomaly"]

    assert "top_error" in data["profile"]
    assert "error_counts" in data["profile"]

    assert "total_events" in data["pipeline"]
    assert "throughput_eps" in data["pipeline"]
    assert "average_latency_ms" in data["pipeline"]
    assert "dlq_rate" in data["pipeline"]
    assert "status" in data["pipeline"]

    assert isinstance(data["system"], dict)
    assert isinstance(data["recovery"], dict)
    assert isinstance(data["overall_status"], str)
def test_pipeline_metrics_integration():
    response = client.get("/pipeline/metrics")

    assert response.status_code == 200

    data = response.json()

    expected_fields = {
        "total_events",
        "valid_events",
        "invalid_events",
        "throughput_eps",
        "average_latency_ms",
        "p50_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
        "performance_history",
        "latest_snapshot",
    }

    assert expected_fields.issubset(data.keys())

    assert isinstance(data["performance_history"], list)


def test_pipeline_status_integration():
    response = client.get("/pipeline/status")

    assert response.status_code == 200

    data = response.json()

    expected_fields = {
        "status",
        "throughput_eps",
        "average_latency_ms",
    }

    assert expected_fields.issubset(data.keys())


def test_pipeline_api_consistency():
    metrics_response = client.get("/pipeline/metrics")
    status_response = client.get("/pipeline/status")

    assert metrics_response.status_code == 200
    assert status_response.status_code == 200

    metrics = metrics_response.json()
    status = status_response.json()

    assert (
        metrics["throughput_eps"]
        == status["throughput_eps"]
    )

    assert (
        metrics["average_latency_ms"]
        == status["average_latency_ms"]
    )
def test_event_to_observability_integration():
    from alert_server.main import (
        pipeline_latencies_ms,
        quality_history,
        quality_metrics,
    )

    # Capture the current state so the test works
    # regardless of previous test activity.
    initial_total_events = quality_metrics.total_events
    initial_history_size = len(quality_history)
    initial_latency_size = len(pipeline_latencies_ms)

    event = {
        "event_id": "integration-event-001",
        "event_type": "checkout",
        "timestamp": "2026-09-22T09:00:00Z",
        "order_id": "integration-order-001",
        "customer_id": "integration-customer-001",
        "product_id": "integration-product-001",
        "quantity": 1,
        "amount": 100,
        "currency": "INR",
    }

    response = client.post("/events", json=event)

    assert response.status_code == 200
    assert response.json()["valid"] is True

    # Quality engine received the event.
    assert quality_metrics.total_events == initial_total_events + 1

    # Pipeline latency was recorded.
    assert len(pipeline_latencies_ms) >= initial_latency_size + 1

    # A quality history snapshot was created.
    assert len(quality_history) >= initial_history_size + 1

    # Verify the APIs now expose the updated state.
    quality_response = client.get("/quality/status")
    pipeline_response = client.get("/pipeline/metrics")
    live_response = client.get("/observability/live")
    overview_response = client.get("/observability/overview")

    assert quality_response.status_code == 200
    assert pipeline_response.status_code == 200
    assert live_response.status_code == 200
    assert overview_response.status_code == 200

    quality = quality_response.json()
    pipeline = pipeline_response.json()
    live = live_response.json()
    overview = overview_response.json()

    assert quality["total_events"] >= initial_total_events + 1
    assert pipeline["total_events"] >= initial_total_events + 1
    assert live["total_events"] >= initial_total_events + 1

    assert overview["quality"]["valid_events"] >= 1
    assert overview["pipeline"]["total_events"] >= initial_total_events + 1
def test_websocket_integration():
    event = {
        "event_id": "integration-ws-001",
        "event_type": "checkout",
        "timestamp": "2026-09-22T09:10:00Z",
        "order_id": "integration-ws-order-001",
        "customer_id": "integration-ws-customer-001",
        "product_id": "integration-ws-product-001",
        "quantity": 1,
        "amount": 100,
        "currency": "INR",
    }

    with client.websocket_connect(
        "/ws/alerts",
        headers={"origin": "http://localhost:5173"},
    ) as websocket:

        response = client.post(
            "/events",
            json=event,
        )

        assert response.status_code == 200
        assert response.json()["valid"] is True

        messages = []

        for _ in range(10):
            message = websocket.receive_json()
            messages.append(message)

            if message.get("type") == "OBSERVABILITY_OVERVIEW":
                break

        assert messages

        message_types = {
            message.get("type")
            for message in messages
        }

        expected_types = {
            "QUALITY_METRICS",
            "PIPELINE_METRICS",
            "OBSERVABILITY_OVERVIEW",
        }

        assert message_types.intersection(expected_types)

        for message in messages:
            assert "type" in message

            if message["type"] == "LIVE_METRICS":
                assert "timestamp" in message
                assert "data" in message
def test_invalid_event_integration():
    from alert_server.main import quality_metrics

    initial_invalid_events = quality_metrics.invalid_events

    invalid_event = {
        "event_id": "integration-invalid-001",
        "event_type": "checkout",
        "timestamp": "2026-09-22T09:20:00Z",
        "order_id": "integration-invalid-order-001",
        "customer_id": "integration-invalid-customer-001",
        "product_id": "integration-invalid-product-001",

        # Invalid quantity
        "quantity": -5,

        "amount": 100,
        "currency": "INR",
    }

    response = client.post(
        "/events",
        json=invalid_event,
    )

    assert response.status_code == 200

    data = response.json()

    assert data["valid"] is False

    assert quality_metrics.invalid_events >= initial_invalid_events + 1

    quality_response = client.get("/quality/status")
    profile_response = client.get("/quality/profile")
    overview_response = client.get("/observability/overview")

    assert quality_response.status_code == 200
    assert profile_response.status_code == 200
    assert overview_response.status_code == 200

    quality = quality_response.json()
    profile = profile_response.json()
    overview = overview_response.json()

    assert quality["invalid_events"] >= initial_invalid_events + 1
    assert profile["invalid_events"] >= initial_invalid_events + 1

    assert "error_counts" in profile
    assert "invalid_events" in overview["quality"]
def test_anomaly_detection_integration():
    from alert_server.main import quality_metrics

    initial_total = quality_metrics.total_events

    # Create several invalid events to significantly reduce quality.
    for index in range(5):
        invalid_event = {
            "event_id": f"integration-anomaly-{index}",
            "event_type": "checkout",
            "timestamp": "2026-09-22T09:30:00Z",
            "order_id": f"integration-anomaly-order-{index}",
            "customer_id": f"integration-anomaly-customer-{index}",
            "product_id": f"integration-anomaly-product-{index}",
            "quantity": -10,
            "amount": 100,
            "currency": "INR",
        }

        response = client.post(
            "/events",
            json=invalid_event,
        )

        assert response.status_code == 200
        assert response.json()["valid"] is False

    assert quality_metrics.total_events >= initial_total + 5

    anomaly_response = client.get("/quality/anomaly")
    status_response = client.get("/quality/status")
    overview_response = client.get("/observability/overview")

    assert anomaly_response.status_code == 200
    assert status_response.status_code == 200
    assert overview_response.status_code == 200

    anomaly = anomaly_response.json()
    status = status_response.json()
    overview = overview_response.json()

    assert "is_anomaly" in anomaly
    assert "severity" in anomaly
    assert "current_quality" in anomaly
    assert "baseline_quality" in anomaly
    assert "deviation" in anomaly

    assert "is_anomaly" in status
    assert "anomaly_severity" in status

    assert "anomaly" in overview
    assert "is_anomaly" in overview["anomaly"]
    assert "severity" in overview["anomaly"]
def test_pipeline_degradation_integration(monkeypatch):
    from alert_server import main

    original_builder = main.build_pipeline_metrics

    def degraded_pipeline_metrics(*args, **kwargs):
        metrics = original_builder(*args, **kwargs)

        metrics.throughput_eps = 2.0
        metrics.average_latency_ms = 750.0

        return metrics

    monkeypatch.setattr(
        main,
        "build_pipeline_metrics",
        degraded_pipeline_metrics,
    )

    response = client.get("/pipeline/status")

    assert response.status_code == 200

    status = response.json()

    assert status["throughput_eps"] == 2.0
    assert status["average_latency_ms"] == 750.0
    assert status["status"] == "DEGRADED"

    overview_response = client.get(
        "/observability/overview"
    )

    assert overview_response.status_code == 200

    overview = overview_response.json()

    assert overview["pipeline"]["throughput_eps"] == 2.0
    assert overview["pipeline"]["average_latency_ms"] == 750.0
    assert overview["pipeline"]["status"] == "DEGRADED"
def test_kafka_failure_integration():
    response = client.post(
        "/system/components/kafka",
        json={
            "status": "DOWN",
            "message": "Kafka connection unavailable",
        },
    )

    assert response.status_code == 200

    system_response = client.get("/system/status")
    overview_response = client.get("/observability/overview")

    assert system_response.status_code == 200
    assert overview_response.status_code == 200

    system = system_response.json()
    overview = overview_response.json()

    assert "health" in system
    assert "kafka" in system["health"]

    assert system["health"]["kafka"]["status"] == "FAILED"
    assert overview["overall_status"] == "CRITICAL"

    # Restore Kafka for subsequent tests.
    restore_response = client.post(
        "/system/components/kafka",
        json={
            "status": "UP",
            "message": "Kafka restored",
        },
    )

    assert restore_response.status_code == 200
def test_kafka_recovery_integration():
    # Put Kafka into a failed state.
    failure_response = client.post(
        "/system/components/kafka",
        json={
            "status": "DOWN",
            "message": "Kafka connection unavailable",
        },
    )

    assert failure_response.status_code == 200

    failed_overview = client.get("/observability/overview")

    assert failed_overview.status_code == 200
    assert failed_overview.json()["overall_status"] == "CRITICAL"

    # Recover Kafka.
    recovery_response = client.post(
        "/system/components/kafka",
        json={
            "status": "UP",
            "message": "Kafka connection restored",
        },
    )

    assert recovery_response.status_code == 200

    system_response = client.get("/system/status")

    assert system_response.status_code == 200

    system = system_response.json()

    assert system["health"]["kafka"]["status"] == "HEALTHY"
def test_circuit_breaker_integration():
    from alert_server.main import (
        circuit_breaker,
        execute_with_resilience,
    )

    original_state = circuit_breaker.state
    original_failure_count = circuit_breaker.failure_count

    def failing_operation():
        raise RuntimeError("simulated backend failure")

    try:
        # Trigger enough failures to open the circuit.
        for _ in range(5):
            try:
                execute_with_resilience(failing_operation)
            except Exception:
                pass

        assert circuit_breaker.state == "OPEN"

        # Once OPEN, the circuit should reject the operation.
        try:
            execute_with_resilience(lambda: "should not execute")
            assert False, "Circuit breaker should reject the call"
        except Exception:
            pass

        # Verify recovery metrics are exposed.
        overview_response = client.get(
            "/observability/overview"
        )

        assert overview_response.status_code == 200

        overview = overview_response.json()

        assert "recovery" in overview
        assert isinstance(overview["recovery"], dict)

    finally:
        circuit_breaker.state = original_state
        circuit_breaker.failure_count = original_failure_count