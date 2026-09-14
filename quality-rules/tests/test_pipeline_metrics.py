from pipeline_metrics import (
    build_pipeline_metrics,
    calculate_average_latency,
    calculate_dlq_rate,
    calculate_throughput,
    get_pipeline_status,
)


def test_calculate_throughput():
    assert calculate_throughput(1000, 20) == 50.0


def test_calculate_throughput_zero_elapsed():
    assert calculate_throughput(100, 0) == 0.0


def test_calculate_average_latency():
    assert calculate_average_latency([100, 200, 300]) == 200.0


def test_calculate_average_latency_empty():
    assert calculate_average_latency([]) == 0.0


def test_calculate_dlq_rate():
    assert calculate_dlq_rate(30, 1000) == 3.0


def test_calculate_dlq_rate_zero_events():
    assert calculate_dlq_rate(0, 0) == 0.0


def test_pipeline_status_healthy():
    assert get_pipeline_status(50, 180) == "HEALTHY"


def test_pipeline_status_degraded():
    assert get_pipeline_status(50, 650) == "DEGRADED"


def test_pipeline_status_critical():
    assert get_pipeline_status(50, 1500) == "CRITICAL"


def test_build_pipeline_metrics():
    metrics = build_pipeline_metrics(
        total_events=1000,
        valid_events=970,
        invalid_events=30,
        elapsed_seconds=20,
        latencies_ms=[100, 200, 150],
    )

    assert metrics.total_events == 1000
    assert metrics.valid_events == 970
    assert metrics.invalid_events == 30
    assert metrics.throughput_eps == 50.0
    assert metrics.average_latency_ms == 150.0
    assert metrics.dlq_rate == 3.0
