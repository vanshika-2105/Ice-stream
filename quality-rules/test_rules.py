from rules import validate_event


def test_valid_event():
    event = {
        "event_id": "evt-001",
        "timestamp": "2026-08-29T10:00:00Z",
        "user_id": "user-123",
        "amount": 499.99,
        "currency": "INR",
    }

    errors = validate_event(event)

    assert errors == []


def test_missing_event_id():
    event = {
        "timestamp": "2026-08-29T10:00:00Z",
        "user_id": "user-123",
        "amount": 499.99,
        "currency": "INR",
    }

    errors = validate_event(event)

    assert "event_id_required" in errors


def test_negative_amount():
    event = {
        "event_id": "evt-002",
        "timestamp": "2026-08-29T10:00:00Z",
        "user_id": "user-123",
        "amount": -100,
        "currency": "INR",
    }

    errors = validate_event(event)

    assert "amount_must_be_non_negative" in errors