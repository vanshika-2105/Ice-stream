HEALTHY_THRESHOLD = 95.0
CRITICAL_THRESHOLD = 90.0


def get_quality_severity(quality_score: float) -> str | None:
    """
    Determine alert severity from the quality score.

    95%+       -> no alert
    90-94.99%  -> WARNING
    below 90%  -> CRITICAL
    """

    if quality_score >= HEALTHY_THRESHOLD:
        return None

    if quality_score >= CRITICAL_THRESHOLD:
        return "WARNING"

    return "CRITICAL"