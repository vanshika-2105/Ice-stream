from metric_validation import validate_metrics


def test_valid_metrics():
    metrics = {
        "total_events": 100,
        "valid_events": 93,
        "invalid_events": 7,
        "quality_score": 93.0,
        "invalid_event_rate": 7.0,
    }

    valid, errors = validate_metrics(metrics)

    assert valid is True
    assert errors == []


def test_invalid_event_counts():
    metrics = {
        "total_events": 100,
        "valid_events": 95,
        "invalid_events": 10,
        "quality_score": 95.0,
        "invalid_event_rate": 10.0,
    }

    valid, errors = validate_metrics(metrics)

    assert valid is False
    assert len(errors) > 0


def test_invalid_quality_score():
    metrics = {
        "total_events": 100,
        "valid_events": 90,
        "invalid_events": 10,
        "quality_score": 120.0,
        "invalid_event_rate": 10.0,
    }

    valid, errors = validate_metrics(metrics)

    assert valid is False


def test_invalid_event_rate():
    metrics = {
        "total_events": 100,
        "valid_events": 90,
        "invalid_events": 10,
        "quality_score": 90.0,
        "invalid_event_rate": -5.0,
    }

    valid, errors = validate_metrics(metrics)

    assert valid is False