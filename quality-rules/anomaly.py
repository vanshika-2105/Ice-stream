from dataclasses import dataclass
from statistics import mean


ANOMALY_WARNING_THRESHOLD = 5.0
ANOMALY_CRITICAL_THRESHOLD = 10.0
ANOMALY_WINDOW_SIZE = 5


@dataclass
class AnomalyResult:
    """Represent the result of quality anomaly detection."""

    is_anomaly: bool
    current_quality: float
    baseline_quality: float
    deviation: float
    reason: str
    severity: str


def calculate_baseline(history: list[float]) -> float:
    """Calculate the baseline from the most recent quality scores."""

    if not history:
        return 0.0

    recent_history = history[-ANOMALY_WINDOW_SIZE:]

    return mean(recent_history)


def detect_anomaly(
    current_quality: float,
    history: list[float],
) -> AnomalyResult:
    """Detect quality anomalies and classify their severity."""

    if len(history) < 2:
        return AnomalyResult(
            is_anomaly=False,
            current_quality=current_quality,
            baseline_quality=0.0,
            deviation=0.0,
            reason="Insufficient historical data",
            severity="INFO",
        )

    baseline = calculate_baseline(history)

    deviation = baseline - current_quality

    if deviation > ANOMALY_CRITICAL_THRESHOLD:
        is_anomaly = True
        severity = "CRITICAL"
        reason = (
            "Quality dropped significantly below historical baseline"
        )

    elif deviation >= ANOMALY_WARNING_THRESHOLD:
        is_anomaly = True
        severity = "WARNING"
        reason = (
            "Quality dropped significantly below historical baseline"
        )

    else:
        is_anomaly = False
        severity = "INFO"
        reason = "Quality is within expected range"

    return AnomalyResult(
        is_anomaly=is_anomaly,
        current_quality=current_quality,
        baseline_quality=baseline,
        deviation=deviation,
        reason=reason,
        severity=severity,
    )