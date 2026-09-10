from anomaly import detect_anomaly


def test_normal_quality_is_not_anomaly():
    history = [98, 97, 98, 97, 96]

    result = detect_anomaly(97, history)

    assert result.is_anomaly is False
    assert result.current_quality == 97
    assert result.baseline_quality == 97.2
    assert result.deviation < 5.0
    assert result.reason == "Quality is within expected range"


def test_sudden_quality_drop_is_anomaly():
    history = [98, 97, 98, 97, 96]

    result = detect_anomaly(82, history)

    assert result.is_anomaly is True
    assert result.current_quality == 82
    assert result.baseline_quality == 97.2
    assert result.deviation >= 5.0
    assert (
        result.reason
        == "Quality dropped significantly below historical baseline"
    )


def test_small_quality_drop_is_not_anomaly():
    history = [98, 97, 98, 97, 96]

    result = detect_anomaly(94, history)

    assert result.is_anomaly is False
    assert result.current_quality == 94
    assert result.baseline_quality == 97.2
    assert result.deviation < 5.0
    assert result.reason == "Quality is within expected range"


def test_insufficient_history():
    result = detect_anomaly(98, [97])

    assert result.is_anomaly is False
    assert result.baseline_quality == 0.0
    assert result.deviation == 0.0
    assert result.reason == "Insufficient historical data"


def test_empty_history():
    result = detect_anomaly(98, [])

    assert result.is_anomaly is False
    assert result.baseline_quality == 0.0
    assert result.deviation == 0.0
    assert result.reason == "Insufficient historical data"


def test_baseline_uses_latest_five_values():
    history = [50, 50, 98, 97, 98, 97, 96]

    result = detect_anomaly(96, history)

    assert result.baseline_quality == 97.2