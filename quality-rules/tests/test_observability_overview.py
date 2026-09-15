from fastapi.testclient import TestClient

import alert_server.main as main


client = TestClient(main.app)


def reset_state():
    """Reset observability state before each test."""

    main.quality_metrics.total_events = 0
    main.quality_metrics.valid_events = 0
    main.quality_metrics.invalid_events = 0
    main.quality_metrics.error_counts.clear()

    main.alert_engine.current_status = "HEALTHY"
    main.alert_engine.anomaly_active = False
    main.alert_engine.anomaly_severity = None

    main.quality_history.clear()

    main.profile_event_type_counts.clear()
    main.profile_currency_counts.clear()

    main.pipeline_start_time = None
    main.pipeline_elapsed_seconds = 0.0
    main.pipeline_latencies_ms.clear()
    main.pipeline_status = "HEALTHY"


def test_observability_overview_contains_all_sections():
    reset_state()

    response = client.get("/observability/overview")

    assert response.status_code == 200

    data = response.json()

    assert "quality" in data
    assert "anomaly" in data
    assert "profile" in data
    assert "pipeline" in data
    assert "system" in data
    assert "overall_status" in data


def test_observability_overview_healthy():
    reset_state()

    main.quality_metrics.total_events = 100
    main.quality_metrics.valid_events = 100

    main.pipeline_elapsed_seconds = 1.0
    main.pipeline_latencies_ms.extend([100.0, 120.0])

    response = client.get("/observability/overview")

    assert response.status_code == 200

    data = response.json()

    assert data["quality"]["status"] == "HEALTHY"
    assert data["pipeline"]["status"] == "HEALTHY"
    assert data["overall_status"] == "HEALTHY"


def test_observability_overview_degraded():
    reset_state()

    main.quality_metrics.total_events = 100
    main.quality_metrics.valid_events = 92
    main.quality_metrics.invalid_events = 8

    main.pipeline_elapsed_seconds = 10.0
    main.pipeline_latencies_ms.extend([600.0, 700.0])

    response = client.get("/observability/overview")

    assert response.status_code == 200

    data = response.json()

    assert data["quality"]["status"] == "WARNING"
    assert data["pipeline"]["status"] == "DEGRADED"
    assert data["overall_status"] == "DEGRADED"


def test_observability_overview_critical():
    reset_state()

    main.quality_metrics.total_events = 100
    main.quality_metrics.valid_events = 50
    main.quality_metrics.invalid_events = 50

    main.pipeline_elapsed_seconds = 100.0
    main.pipeline_latencies_ms.extend([1200.0, 1500.0])

    response = client.get("/observability/overview")

    assert response.status_code == 200

    data = response.json()

    assert data["quality"]["status"] == "CRITICAL"
    assert data["pipeline"]["status"] == "CRITICAL"
    assert data["overall_status"] == "CRITICAL"


def test_critical_takes_priority_over_degraded():
    reset_state()

    assert (
        main.calculate_overall_status(
            ["DEGRADED", "CRITICAL"]
        )
        == "CRITICAL"
    )


def test_degraded_takes_priority_over_healthy():
    reset_state()

    assert (
        main.calculate_overall_status(
            ["HEALTHY", "DEGRADED"]
        )
        == "DEGRADED"
    )