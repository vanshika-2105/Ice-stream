from alerts import AlertEngine


def test_healthy_quality():
    engine = AlertEngine()

    alert = engine.evaluate(98.0)

    assert engine.current_status == "HEALTHY"
    assert alert is None


def test_warning_quality():
    engine = AlertEngine()

    alert = engine.evaluate(93.0)

    assert engine.current_status == "WARNING"
    assert alert is not None
    assert alert.type == "QUALITY_ALERT"
    assert alert.severity == "WARNING"
    assert alert.quality_score == 93.0


def test_critical_quality():
    engine = AlertEngine()

    alert = engine.evaluate(85.0)

    assert engine.current_status == "CRITICAL"
    assert alert is not None
    assert alert.type == "QUALITY_ALERT"
    assert alert.severity == "CRITICAL"
    assert alert.quality_score == 85.0


def test_warning_deduplication():
    engine = AlertEngine()

    first_alert = engine.evaluate(93.0)
    second_alert = engine.evaluate(92.0)
    third_alert = engine.evaluate(91.0)

    assert first_alert is not None
    assert first_alert.severity == "WARNING"

    assert second_alert is None
    assert third_alert is None

    assert engine.current_status == "WARNING"


def test_critical_deduplication():
    engine = AlertEngine()

    first_alert = engine.evaluate(85.0)
    second_alert = engine.evaluate(80.0)
    third_alert = engine.evaluate(75.0)

    assert first_alert is not None
    assert first_alert.severity == "CRITICAL"

    assert second_alert is None
    assert third_alert is None

    assert engine.current_status == "CRITICAL"


def test_warning_to_critical_transition():
    engine = AlertEngine()

    warning_alert = engine.evaluate(93.0)
    critical_alert = engine.evaluate(85.0)

    assert warning_alert is not None
    assert warning_alert.severity == "WARNING"

    assert critical_alert is not None
    assert critical_alert.severity == "CRITICAL"

    assert engine.current_status == "CRITICAL"


def test_critical_to_healthy_recovery():
    engine = AlertEngine()

    critical_alert = engine.evaluate(85.0)
    recovery = engine.evaluate(97.0)

    assert critical_alert is not None
    assert critical_alert.severity == "CRITICAL"

    assert recovery is not None
    assert recovery.type == "QUALITY_RECOVERY"
    assert recovery.previous_status == "CRITICAL"
    assert recovery.current_status == "HEALTHY"
    assert recovery.quality_score == 97.0

    assert engine.current_status == "HEALTHY"


def test_warning_to_healthy_recovery():
    engine = AlertEngine()

    warning_alert = engine.evaluate(93.0)
    recovery = engine.evaluate(97.0)

    assert warning_alert is not None
    assert warning_alert.severity == "WARNING"

    assert recovery is not None
    assert recovery.type == "QUALITY_RECOVERY"
    assert recovery.previous_status == "WARNING"
    assert recovery.current_status == "HEALTHY"


def test_full_alert_transition():
    engine = AlertEngine()

    healthy = engine.evaluate(98.0)
    warning = engine.evaluate(93.0)
    critical = engine.evaluate(85.0)
    recovery = engine.evaluate(97.0)

    assert healthy is None

    assert warning.type == "QUALITY_ALERT"
    assert warning.severity == "WARNING"

    assert critical.type == "QUALITY_ALERT"
    assert critical.severity == "CRITICAL"

    assert recovery.type == "QUALITY_RECOVERY"
    assert recovery.previous_status == "CRITICAL"
    assert recovery.current_status == "HEALTHY"

    assert engine.current_status == "HEALTHY"