from metrics import QualityMetrics


def test_initial_metrics():
    metrics = QualityMetrics()

    result = metrics.get_metrics()

    assert result["total_events"] == 0
    assert result["valid_events"] == 0
    assert result["invalid_events"] == 0
    assert result["quality_score"] == 0.0
    assert result["errors"] == {}


def test_all_valid_events():
    metrics = QualityMetrics()

    for _ in range(100):
        metrics.record_valid()

    result = metrics.get_metrics()

    assert result["total_events"] == 100
    assert result["valid_events"] == 100
    assert result["invalid_events"] == 0
    assert result["quality_score"] == 100.0


def test_invalid_events():
    metrics = QualityMetrics()

    for _ in range(94):
        metrics.record_valid()

    for _ in range(6):
        metrics.record_invalid([
            {
                "field": "amount",
                "code": "NEGATIVE_VALUE",
                "message": "Amount cannot be negative",
            }
        ])

    result = metrics.get_metrics()

    assert result["total_events"] == 100
    assert result["valid_events"] == 94
    assert result["invalid_events"] == 6
    assert result["quality_score"] == 94.0
    assert result["errors"]["NEGATIVE_VALUE"] == 6


def test_multiple_error_types():
    metrics = QualityMetrics()

    metrics.record_invalid([
        {
            "field": "amount",
            "code": "NEGATIVE_VALUE",
            "message": "Amount cannot be negative",
        },
        {
            "field": "currency",
            "code": "INVALID_CURRENCY",
            "message": "Invalid currency",
        },
    ])

    metrics.record_invalid([
        {
            "field": "quantity",
            "code": "NON_POSITIVE_VALUE",
            "message": "Quantity must be greater than 0",
        }
    ])

    result = metrics.get_metrics()

    assert result["total_events"] == 2
    assert result["valid_events"] == 0
    assert result["invalid_events"] == 2
    assert result["quality_score"] == 0.0

    assert result["errors"]["NEGATIVE_VALUE"] == 1
    assert result["errors"]["INVALID_CURRENCY"] == 1
    assert result["errors"]["NON_POSITIVE_VALUE"] == 1