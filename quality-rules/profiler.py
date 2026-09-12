from dataclasses import dataclass, field


@dataclass
class DataQualityProfile:
    """Represent a profile of processed data quality."""

    total_events: int
    valid_events: int
    invalid_events: int
    error_counts: dict[str, int] = field(default_factory=dict)
    event_type_counts: dict[str, int] = field(default_factory=dict)
    currency_counts: dict[str, int] = field(default_factory=dict)


def build_profile(
    total_events: int,
    valid_events: int,
    invalid_events: int,
    error_counts: dict[str, int] | None = None,
    event_type_counts: dict[str, int] | None = None,
    currency_counts: dict[str, int] | None = None,
) -> DataQualityProfile:
    """Build a data-quality profile from aggregated metrics."""

    return DataQualityProfile(
        total_events=total_events,
        valid_events=valid_events,
        invalid_events=invalid_events,
        error_counts=error_counts or {},
        event_type_counts=event_type_counts or {},
        currency_counts=currency_counts or {},
    )

def calculate_percentage(count: int, total: int) -> float:
    """Calculate a percentage safely."""

    if total == 0:
        return 0.0

    return round((count / total) * 100, 2)

def calculate_error_percentages(
    error_counts: dict[str, int],
) -> dict[str, float]:
    """Calculate the percentage distribution of errors."""

    total_errors = sum(error_counts.values())

    return {
        error_code: calculate_percentage(
            count,
            total_errors,
        )
        for error_code, count in error_counts.items()
    }

def get_top_error(
    error_counts: dict[str, int],
) -> str | None:
    """Return the most frequent error code."""

    if not error_counts:
        return None

    return max(
        error_counts,
        key=error_counts.get,
    )
