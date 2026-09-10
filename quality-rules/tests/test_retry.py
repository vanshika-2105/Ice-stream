import pytest

from alert_server.retry import retry_call


def test_retry_succeeds_on_first_attempt():
    calls = []

    def operation():
        calls.append(1)
        return "success"

    result = retry_call(operation)

    assert result == "success"
    assert len(calls) == 1


def test_retry_succeeds_after_failures(monkeypatch):
    calls = []

    def operation():
        calls.append(1)

        if len(calls) < 3:
            raise RuntimeError("temporary failure")

        return "success"

    monkeypatch.setattr("alert_server.retry.time.sleep", lambda _: None)

    result = retry_call(operation, max_attempts=3)

    assert result == "success"
    assert len(calls) == 3


def test_retry_stops_after_max_attempts(monkeypatch):
    calls = []

    def operation():
        calls.append(1)
        raise RuntimeError("permanent failure")

    monkeypatch.setattr("alert_server.retry.time.sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="permanent failure"):
        retry_call(operation, max_attempts=3)

    assert len(calls) == 3


def test_retry_does_not_retry_when_max_attempts_is_one():
    calls = []

    def operation():
        calls.append(1)
        raise RuntimeError("failure")

    with pytest.raises(RuntimeError):
        retry_call(operation, max_attempts=1)

    assert len(calls) == 1


def test_invalid_max_attempts():
    def operation():
        return "success"

    with pytest.raises(ValueError):
        retry_call(operation, max_attempts=0)