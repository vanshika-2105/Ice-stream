from thresholds import get_quality_severity


def test_healthy_quality():
    assert get_quality_severity(100.0) is None
    assert get_quality_severity(95.0) is None


def test_warning_quality():
    assert get_quality_severity(94.0) == "WARNING"
    assert get_quality_severity(90.0) == "WARNING"


def test_critical_quality():
    assert get_quality_severity(89.0) == "CRITICAL"
    assert get_quality_severity(50.0) == "CRITICAL"