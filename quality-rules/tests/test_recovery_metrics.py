import sys
from pathlib import Path


# Add project root to Python path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))


from alert_server.recovery_metrics import RecoveryMetrics


def test_recovery_metrics_start_empty():
    metrics = RecoveryMetrics()

    data = metrics.get_metrics()

    assert data["recovery_count"] == 0
    assert data["failure_count"] == 0
    assert data["retry_count"] == 0
    assert data["current_circuit_state"] == "CLOSED"
    assert data["last_failure_time"] is None
    assert data["last_recovery_time"] is None
    assert metrics.get_history() == []


def test_failure_is_recorded():
    metrics = RecoveryMetrics()

    metrics.record_failure("kafka")

    data = metrics.get_metrics()

    assert data["failure_count"] == 1
    assert data["last_failure_time"] is not None

    history = metrics.get_history()

    assert len(history) == 1
    assert history[0]["event"] == "FAILURE"
    assert history[0]["component"] == "kafka"


def test_retry_is_recorded():
    metrics = RecoveryMetrics()

    metrics.record_retry(
        attempt=1,
        component="kafka",
    )
    metrics.record_retry(
        attempt=2,
        component="kafka",
    )

    data = metrics.get_metrics()

    assert data["retry_count"] == 2

    history = metrics.get_history()

    assert len(history) == 2
    assert history[0]["event"] == "RETRY"
    assert history[0]["attempt"] == 1
    assert history[1]["attempt"] == 2


def test_circuit_transition_is_recorded():
    metrics = RecoveryMetrics()

    metrics.record_circuit_transition(
        "OPEN",
        "kafka",
    )

    data = metrics.get_metrics()

    assert data["current_circuit_state"] == "OPEN"

    history = metrics.get_history()

    assert len(history) == 1
    assert history[0]["event"] == "CIRCUIT_TRANSITION"
    assert history[0]["state"] == "OPEN"


def test_recovery_is_recorded():
    metrics = RecoveryMetrics()

    metrics.record_recovery("kafka")

    data = metrics.get_metrics()

    assert data["recovery_count"] == 1
    assert data["last_recovery_time"] is not None

    history = metrics.get_history()

    assert len(history) == 1
    assert history[0]["event"] == "RECOVERY"
    assert history[0]["component"] == "kafka"
    assert history[0]["status"] == "RECOVERED"