from dataclasses import dataclass
from statistics import mean


ANOMALY_DROP_THRESHOLD = 5.0
ANOMALY_WINDOW_SIZE = 5


@dataclass
class AnomalyResult:
    """Represent the result of quality anomaly detection."""

    is_anomaly: bool
    current_quality: float
    baseline_quality: float
    deviation: float
    reason: str


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
    """Detect whether current quality significantly differs from history."""

    if len(history) < 2:
        return AnomalyResult(
            is_anomaly=False,
            current_quality=current_quality,
            baseline_quality=0.0,
            deviation=0.0,
            reason="Insufficient historical data",
        )

    baseline = calculate_baseline(history)

    deviation = baseline - current_quality

    is_anomaly = deviation >= ANOMALY_DROP_THRESHOLD

    if is_anomaly:
        reason = (
            "Quality dropped significantly below historical baseline"
        )
    else:
        reason = "Quality is within expected range"

    return AnomalyResult(
        is_anomaly=is_anomaly,
        current_quality=current_quality,
        baseline_quality=baseline,
        deviation=deviation,
        reason=reason,
    )