
from validator import validate_checkout_event


def valid_event():
    return {
        "event_id": "evt-001",
        "event_type": "checkout",
        "timestamp": "2026-08-30T10:30:00Z",
        "order_id": "ord-001",
        "customer_id": "cust-001",
        "product_id": "prod-001",
        "quantity": 2,
        "amount": 499.99,
        "currency": "INR",
    }


def test_valid_checkout_event():
    result = validate_checkout_event(valid_event())

    assert result["valid"] is True
    assert result["errors"] == []


def test_missing_required_field():
    event = valid_event()
    del event["event_id"]

    result = validate_checkout_event(event)

    assert result["valid"] is False
    assert any(
        error["field"] == "event_id"
        and error["code"] == "REQUIRED_FIELD"
        for error in result["errors"]
    )


def test_negative_amount():
    event = valid_event()
    event["amount"] = -100

    result = validate_checkout_event(event)

    assert result["valid"] is False
    assert any(
        error["field"] == "amount"
        and error["code"] == "NEGATIVE_VALUE"
        for error in result["errors"]
    )


def test_invalid_quantity():
    event = valid_event()
    event["quantity"] = 0

    result = validate_checkout_event(event)

    assert result["valid"] is False
    assert any(
        error["field"] == "quantity"
        and error["code"] == "NON_POSITIVE_VALUE"
        for error in result["errors"]
    )


def test_invalid_currency():
    event = valid_event()
    event["currency"] = "XYZ"

    result = validate_checkout_event(event)

    assert result["valid"] is False
    assert any(
        error["field"] == "currency"
        and error["code"] == "INVALID_CURRENCY"
        for error in result["errors"]
    )


def test_invalid_timestamp():
    event = valid_event()
    event["timestamp"] = "hello"

    result = validate_checkout_event(event)

    assert result["valid"] is False
    assert any(
        error["field"] == "timestamp"
        and error["code"] == "INVALID_TIMESTAMP"
        for error in result["errors"]
    )


def test_wrong_data_type():
    event = valid_event()
    event["quantity"] = "two"

    result = validate_checkout_event(event)

    assert result["valid"] is False
    assert any(
        error["field"] == "quantity"
        and error["code"] == "INVALID_TYPE"
        for error in result["errors"]
    )


def test_invalid_event_type():
    event = valid_event()
    event["event_type"] = "payment"

    result = validate_checkout_event(event)

    assert result["valid"] is False
    assert any(
        error["field"] == "event_type"
        and error["code"] == "INVALID_EVENT_TYPE"
        for error in result["errors"]
    )