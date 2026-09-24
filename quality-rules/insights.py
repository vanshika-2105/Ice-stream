from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


# --------------------------------------------------
# Insight types
# --------------------------------------------------

QUALITY_DROP = "QUALITY_DROP"
QUALITY_IMPROVEMENT = "QUALITY_IMPROVEMENT"

LATENCY_INCREASE = "LATENCY_INCREASE"
LATENCY_IMPROVEMENT = "LATENCY_IMPROVEMENT"

THROUGHPUT_DROP = "THROUGHPUT_DROP"
THROUGHPUT_INCREASE = "THROUGHPUT_INCREASE"

DLQ_INCREASE = "DLQ_INCREASE"
DLQ_DECREASE = "DLQ_DECREASE"

ERROR_SPIKE = "ERROR_SPIKE"
ANOMALY_DETECTED = "ANOMALY_DETECTED"


# --------------------------------------------------
# Configurable insight thresholds
# --------------------------------------------------

DEFAULT_QUALITY_CHANGE_THRESHOLD = 1.0
DEFAULT_LATENCY_CHANGE_THRESHOLD_MS = 50.0
DEFAULT_THROUGHPUT_CHANGE_THRESHOLD_EPS = 1.0
DEFAULT_DLQ_CHANGE_THRESHOLD_PERCENT = 1.0
DEFAULT_ERROR_SPIKE_MULTIPLIER = 2.0


# --------------------------------------------------
# Insight model
# --------------------------------------------------

@dataclass
class Insight:
    """Represent a meaningful observation from existing metrics."""

    type: str
    severity: str
    message: str
    value: float | None = None
    previous_value: float | None = None
    created_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert the insight into an API-friendly dictionary."""

        timestamp = self.created_at

        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        return {
            "type": self.type,
            "severity": self.severity,
            "message": self.message,
            "value": self.value,
            "previous_value": self.previous_value,
            "created_at": timestamp.astimezone(
                timezone.utc
            ).isoformat(),
        }


# --------------------------------------------------
# Quality insights
# --------------------------------------------------

def generate_quality_insight(
    previous_quality: float,
    current_quality: float,
    threshold: float = DEFAULT_QUALITY_CHANGE_THRESHOLD,
) -> Insight | None:
    """Generate an insight when quality changes significantly."""

    change = current_quality - previous_quality

    if change <= -threshold:
        return Insight(
            type=QUALITY_DROP,
            severity="WARNING",
            message=(
                f"Data quality dropped by "
                f"{abs(change):g} percentage points."
            ),
            value=current_quality,
            previous_value=previous_quality,
        )

    if change >= threshold:
        return Insight(
            type=QUALITY_IMPROVEMENT,
            severity="INFO",
            message=(
                f"Data quality improved by "
                f"{change:g} percentage points."
            ),
            value=current_quality,
            previous_value=previous_quality,
        )

    return None


# --------------------------------------------------
# Latency insights
# --------------------------------------------------

def generate_latency_insight(
    previous_latency: float,
    current_latency: float,
    threshold: float = DEFAULT_LATENCY_CHANGE_THRESHOLD_MS,
) -> Insight | None:
    """Generate an insight when latency changes significantly."""

    change = current_latency - previous_latency

    if change >= threshold:
        return Insight(
            type=LATENCY_INCREASE,
            severity="WARNING",
            message=(
                f"Pipeline latency increased by "
                f"{change:g} ms."
            ),
            value=current_latency,
            previous_value=previous_latency,
        )

    if change <= -threshold:
        return Insight(
            type=LATENCY_IMPROVEMENT,
            severity="INFO",
            message=(
                f"Pipeline latency decreased by "
                f"{abs(change):g} ms."
            ),
            value=current_latency,
            previous_value=previous_latency,
        )

    return None


# --------------------------------------------------
# Throughput insights
# --------------------------------------------------

def generate_throughput_insight(
    previous_throughput: float,
    current_throughput: float,
    threshold: float = DEFAULT_THROUGHPUT_CHANGE_THRESHOLD_EPS,
) -> Insight | None:
    """Generate an insight when throughput changes significantly."""

    change = current_throughput - previous_throughput

    if change <= -threshold:
        return Insight(
            type=THROUGHPUT_DROP,
            severity="WARNING",
            message=(
                f"Pipeline throughput decreased from "
                f"{previous_throughput:g} EPS to "
                f"{current_throughput:g} EPS."
            ),
            value=current_throughput,
            previous_value=previous_throughput,
        )

    if change >= threshold:
        return Insight(
            type=THROUGHPUT_INCREASE,
            severity="INFO",
            message=(
                f"Pipeline throughput increased from "
                f"{previous_throughput:g} EPS to "
                f"{current_throughput:g} EPS."
            ),
            value=current_throughput,
            previous_value=previous_throughput,
        )

    return None


# --------------------------------------------------
# DLQ insights
# --------------------------------------------------

def generate_dlq_insight(
    previous_dlq_rate: float,
    current_dlq_rate: float,
    threshold: float = DEFAULT_DLQ_CHANGE_THRESHOLD_PERCENT,
) -> Insight | None:
    """Generate an insight when DLQ rate changes significantly."""

    change = current_dlq_rate - previous_dlq_rate

    if change >= threshold:
        return Insight(
            type=DLQ_INCREASE,
            severity="WARNING",
            message=(
                f"DLQ rate increased from "
                f"{previous_dlq_rate:g}% to "
                f"{current_dlq_rate:g}%."
            ),
            value=current_dlq_rate,
            previous_value=previous_dlq_rate,
        )

    if change <= -threshold:
        return Insight(
            type=DLQ_DECREASE,
            severity="INFO",
            message=(
                f"DLQ rate decreased from "
                f"{previous_dlq_rate:g}% to "
                f"{current_dlq_rate:g}%."
            ),
            value=current_dlq_rate,
            previous_value=previous_dlq_rate,
        )

    return None


# --------------------------------------------------
# Error spike insight
# --------------------------------------------------

def generate_error_spike_insights(
    previous_errors: dict[str, int],
    current_errors: dict[str, int],
    multiplier: float = DEFAULT_ERROR_SPIKE_MULTIPLIER,
) -> list[Insight]:
    """
    Generate insights for validation errors that increased
    significantly compared with the previous window.
    """

    insights = []

    for error_code, current_count in current_errors.items():
        previous_count = previous_errors.get(error_code, 0)

        if current_count <= 0:
            continue

        if previous_count == 0:
            is_spike = True
        else:
            is_spike = current_count >= previous_count * multiplier

        if not is_spike:
            continue

        insights.append(
            Insight(
                type=ERROR_SPIKE,
                severity="WARNING",
                message=(
                    f"{error_code} errors increased "
                    f"significantly."
                ),
                value=float(current_count),
                previous_value=float(previous_count),
            )
        )

    return insights


# --------------------------------------------------
# Anomaly insight
# --------------------------------------------------

def generate_anomaly_insight(
    is_anomaly: bool,
    severity: str = "WARNING",
    reason: str = "",
) -> Insight | None:
    """Convert an existing anomaly result into an insight."""

    if not is_anomaly:
        return None

    message = (
        reason
        if reason
        else "A significant quality anomaly was detected."
    )

    return Insight(
        type=ANOMALY_DETECTED,
        severity=severity,
        message=message,
    )


# --------------------------------------------------
# Combined insight generation
# --------------------------------------------------

def generate_insights(
    previous_quality: float | None = None,
    current_quality: float | None = None,
    previous_latency: float | None = None,
    current_latency: float | None = None,
    previous_throughput: float | None = None,
    current_throughput: float | None = None,
    previous_dlq_rate: float | None = None,
    current_dlq_rate: float | None = None,
    previous_errors: dict[str, int] | None = None,
    current_errors: dict[str, int] | None = None,
    is_anomaly: bool = False,
    anomaly_severity: str = "WARNING",
    anomaly_reason: str = "",
) -> list[Insight]:
    """Generate insights from existing quality and pipeline metrics."""

    insights: list[Insight] = []

    if (
        previous_quality is not None
        and current_quality is not None
    ):
        insight = generate_quality_insight(
            previous_quality,
            current_quality,
        )
        if insight:
            insights.append(insight)

    if (
        previous_latency is not None
        and current_latency is not None
    ):
        insight = generate_latency_insight(
            previous_latency,
            current_latency,
        )
        if insight:
            insights.append(insight)

    if (
        previous_throughput is not None
        and current_throughput is not None
    ):
        insight = generate_throughput_insight(
            previous_throughput,
            current_throughput,
        )
        if insight:
            insights.append(insight)

    if (
        previous_dlq_rate is not None
        and current_dlq_rate is not None
    ):
        insight = generate_dlq_insight(
            previous_dlq_rate,
            current_dlq_rate,
        )
        if insight:
            insights.append(insight)

    if previous_errors is not None and current_errors is not None:
        insights.extend(
            generate_error_spike_insights(
                previous_errors,
                current_errors,
            )
        )

    anomaly_insight = generate_anomaly_insight(
        is_anomaly=is_anomaly,
        severity=anomaly_severity,
        reason=anomaly_reason,
    )

    if anomaly_insight:
        insights.append(anomaly_insight)

    return insights