import time

from alert_server.circuit_breaker import CircuitBreaker, CircuitState


def test_circuit_starts_closed():
    breaker = CircuitBreaker()

    assert breaker.state == CircuitState.CLOSED
    assert breaker.failure_count == 0
    assert breaker.can_execute() is True


def test_five_failures_open_circuit():
    breaker = CircuitBreaker()

    for _ in range(4):
        breaker.record_failure()

    assert breaker.state == CircuitState.CLOSED

    breaker.record_failure()

    assert breaker.state == CircuitState.OPEN
    assert breaker.failure_count == 5


def test_open_circuit_blocks_calls():
    breaker = CircuitBreaker()

    for _ in range(5):
        breaker.record_failure()

    assert breaker.state == CircuitState.OPEN
    assert breaker.can_execute() is False


def test_timeout_moves_to_half_open():
    breaker = CircuitBreaker(
        failure_threshold=5,
        recovery_timeout=0.1,
    )

    for _ in range(5):
        breaker.record_failure()

    assert breaker.state == CircuitState.OPEN
    assert breaker.can_execute() is False

    time.sleep(0.15)

    assert breaker.can_execute() is True
    assert breaker.state == CircuitState.HALF_OPEN


def test_half_open_success_closes_circuit():
    breaker = CircuitBreaker(
        failure_threshold=5,
        recovery_timeout=0.1,
    )

    for _ in range(5):
        breaker.record_failure()

    time.sleep(0.15)

    assert breaker.can_execute() is True
    assert breaker.state == CircuitState.HALF_OPEN

    breaker.record_success()

    assert breaker.state == CircuitState.CLOSED
    assert breaker.failure_count == 0
    assert breaker.can_execute() is True


def test_half_open_failure_reopens_circuit():
    breaker = CircuitBreaker(
        failure_threshold=5,
        recovery_timeout=0.1,
    )

    for _ in range(5):
        breaker.record_failure()

    time.sleep(0.15)

    assert breaker.can_execute() is True
    assert breaker.state == CircuitState.HALF_OPEN

    breaker.record_failure()

    assert breaker.state == CircuitState.OPEN
    assert breaker.failure_count == 6


def test_success_resets_failure_count():
    breaker = CircuitBreaker()

    breaker.record_failure()
    breaker.record_failure()

    assert breaker.failure_count == 2

    breaker.record_success()

    assert breaker.failure_count == 0
    assert breaker.state == CircuitState.CLOSED