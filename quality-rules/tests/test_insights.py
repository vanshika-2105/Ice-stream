from insights import (
    ANOMALY_DETECTED,
    DLQ_INCREASE,
    ERROR_SPIKE,
    LATENCY_INCREASE,
    QUALITY_DROP,
    QUALITY_IMPROVEMENT,
    THROUGHPUT_DROP,
    generate_anomaly_insight,
    generate_dlq_insight,
    generate_error_spike_insights,
    generate_latency_insight,
    generate_quality_insight,
    generate_throughput_insight,
)


def test_quality_drop():
    insight = generate_quality_insight(97, 91)

    assert insight is not None
    assert insight.type == QUALITY_DROP
    assert insight.severity == "WARNING"
    assert insight.value == 91
    assert insight.previous_value == 97
    assert "6 percentage points" in insight.message


def test_quality_improvement():
    insight = generate_quality_insight(85, 94)

    assert insight is not None
    assert insight.type == QUALITY_IMPROVEMENT
    assert insight.value == 94
    assert insight.previous_value == 85
    assert "9 percentage points" in insight.message


def test_latency_increase():
    insight = generate_latency_insight(200, 600)

    assert insight is not None
    assert insight.type == LATENCY_INCREASE
    assert insight.value == 600
    assert insight.previous_value == 200
    assert "400 ms" in insight.message


def test_throughput_drop():
    insight = generate_throughput_insight(20, 5)

    assert insight is not None
    assert insight.type == THROUGHPUT_DROP
    assert insight.value == 5
    assert insight.previous_value == 20
    assert "20 EPS to 5 EPS" in insight.message


def test_dlq_increase():
    insight = generate_dlq_insight(2, 10)

    assert insight is not None
    assert insight.type == DLQ_INCREASE
    assert insight.value == 10
    assert insight.previous_value == 2
    assert "2% to 10%" in insight.message


def test_error_spike():
    insights = generate_error_spike_insights(
        previous_errors={"INVALID_AMOUNT": 1},
        current_errors={"INVALID_AMOUNT": 5},
    )

    assert len(insights) == 1
    assert insights[0].type == ERROR_SPIKE
    assert insights[0].severity == "WARNING"


def test_anomaly_detected():
    insight = generate_anomaly_insight(
        is_anomaly=True,
        severity="WARNING",
        reason="Quality dropped significantly.",
    )

    assert insight is not None
    assert insight.type == ANOMALY_DETECTED
    assert insight.severity == "WARNING"
    assert insight.message == "Quality dropped significantly."


def test_stable_system():
    quality = generate_quality_insight(96, 96)
    latency = generate_latency_insight(200, 205)
    throughput = generate_throughput_insight(10, 10)
    dlq = generate_dlq_insight(2, 2)

    assert quality is None
    assert latency is None
    assert throughput is None
    assert dlq is None