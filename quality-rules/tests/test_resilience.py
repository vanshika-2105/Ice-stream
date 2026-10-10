import sys
from pathlib import Path

import pytest


# Add project root to Python path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))


from alert_server.circuit_breaker import CircuitBreaker, CircuitState
from alert_server.main import execute_with_resilience


def test_success_closes_circuit():
    result = execute_with_resilience(
        lambda: "success"
    )

    assert result == "success"


def test_failure_records_circuit_failure(monkeypatch):
    from alert_server import main

    main.circuit_breaker = CircuitBreaker(
        failure_threshold=5
    )

    def failing_operation():
        raise RuntimeError("dependency failure")

    monkeypatch.setattr(
        "alert_server.retry.time.sleep",
        lambda _: None,
    )

    with pytest.raises(RuntimeError, match="dependency failure"):
        execute_with_resilience(failing_operation)

    assert main.circuit_breaker.failure_count == 1
    assert main.circuit_breaker.state == CircuitState.CLOSED
def test_repeated_failures_open_circuit(monkeypatch):
    from alert_server import main

    main.circuit_breaker = CircuitBreaker(
        failure_threshold=5
    )

    def failing_operation():
        raise RuntimeError("permanent dependency failure")

    monkeypatch.setattr(
        "alert_server.retry.time.sleep",
        lambda _: None,
    )

    for _ in range(5):
        with pytest.raises(
            RuntimeError,
            match="permanent dependency failure",
        ):
            execute_with_resilience(failing_operation)

    assert main.circuit_breaker.state == CircuitState.OPEN
    assert main.circuit_breaker.failure_count == 5
def test_open_circuit_blocks_operation(monkeypatch):
    from alert_server import main

    main.circuit_breaker = CircuitBreaker(
        failure_threshold=2,
        recovery_timeout=30,
    )

    calls = []

    def failing_operation():
        calls.append(1)
        raise RuntimeError("dependency failure")

    monkeypatch.setattr(
        "alert_server.retry.time.sleep",
        lambda _: None,
    )

    # Cause the circuit to open.
    for _ in range(2):
        with pytest.raises(RuntimeError):
            execute_with_resilience(failing_operation)

    assert main.circuit_breaker.state == CircuitState.OPEN

    # The operation must NOT execute while the circuit is open.
    with pytest.raises(
        Exception,
        match="Dependency temporarily unavailable",
    ):
        execute_with_resilience(failing_operation)

    assert len(calls) == 6
def test_open_circuit_recovers_after_timeout(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from alert_server import main

    main.circuit_breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=0.1,
    )

    monkeypatch.setattr(
        "alert_server.retry.time.sleep",
        lambda _: None,
    )

    def failing_operation():
        raise RuntimeError("dependency failure")

    # First operation fails and opens the circuit.
    with pytest.raises(RuntimeError):
        execute_with_resilience(failing_operation)

    assert main.circuit_breaker.state == CircuitState.OPEN

    # Simulate that the recovery timeout has elapsed.
    main.circuit_breaker.last_failure_time = (
        datetime.now(timezone.utc) - timedelta(seconds=1)
    )

    # The next call should be allowed and move through HALF_OPEN.
    result = execute_with_resilience(
        lambda: "recovered"
    )

    assert result == "recovered"
    assert main.circuit_breaker.state == CircuitState.CLOSED
    assert main.circuit_breaker.failure_count == 0
def test_half_open_failure_reopens_circuit():
    from datetime import datetime, timedelta, timezone
    from alert_server import main

    main.circuit_breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=0.1,
    )

    monkeypatch_time = datetime.now(timezone.utc)

    def failing_operation():
        raise RuntimeError("dependency still unavailable")

    # First failure opens the circuit.
    with pytest.raises(RuntimeError):
        execute_with_resilience(failing_operation)

    assert main.circuit_breaker.state == CircuitState.OPEN

    # Simulate recovery timeout.
    main.circuit_breaker.last_failure_time = (
        monkeypatch_time - timedelta(seconds=1)
    )

    # Verify circuit enters HALF_OPEN.
    assert main.circuit_breaker.can_execute() is True
    assert main.circuit_breaker.state == CircuitState.HALF_OPEN

    # Recovery attempt fails.
    with pytest.raises(RuntimeError):
        execute_with_resilience(failing_operation)

    assert main.circuit_breaker.state == CircuitState.OPEN
def test_system_recovery_emitted_exactly_once():
    from alert_server.system_alerts import SystemAlertEngine

    engine = SystemAlertEngine()

    # Initial failure.
    failure = engine.update_component(
        component="kafka",
        status="DOWN",
        message="Kafka connection failed",
    )

    assert failure is not None
    assert failure.type == "SYSTEM_ALERT"
    assert failure.status == "FAILED"

    # Recovery.
    recovery = engine.update_component(
        component="kafka",
        status="UP",
        message="Kafka connection restored",
    )

    assert recovery is not None
    assert recovery.type == "SYSTEM_RECOVERY"
    assert recovery.component == "kafka"
    assert recovery.status == "RECOVERED"

    # Repeated UP must NOT generate another recovery event.
    duplicate_recovery = engine.update_component(
        component="kafka",
        status="UP",
        message="Kafka connection still healthy",
    )

    assert duplicate_recovery is None

    recovery_events = [
        alert
        for alert in engine.get_alert_history()
        if alert["type"] == "SYSTEM_RECOVERY"
    ]

    assert len(recovery_events) == 1
def test_resilience_updates_recovery_metrics(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from alert_server import main

    main.circuit_breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=0.1,
    )

    # Reset recovery metrics for this test.
    from alert_server.recovery_metrics import RecoveryMetrics

    main.recovery_metrics = RecoveryMetrics()

    monkeypatch.setattr(
        "alert_server.retry.time.sleep",
        lambda _: None,
    )

    def failing_operation():
        raise RuntimeError("dependency failure")

    # Failure should update failure metrics.
    with pytest.raises(RuntimeError):
        main.execute_with_resilience(failing_operation)

    metrics = main.recovery_metrics.get_metrics()

    assert metrics["failure_count"] == 1
    assert metrics["last_failure_time"] is not None
    assert metrics["current_circuit_state"] == "OPEN"

    # Simulate recovery timeout.
    main.circuit_breaker.last_failure_time = (
        datetime.now(timezone.utc) - timedelta(seconds=1)
    )

    # Successful HALF_OPEN operation should record recovery.
    result = main.execute_with_resilience(
        lambda: "recovered"
    )

    assert result == "recovered"

    metrics = main.recovery_metrics.get_metrics()

    assert metrics["recovery_count"] == 1
    assert metrics["last_recovery_time"] is not None
    assert metrics["current_circuit_state"] == "CLOSED"