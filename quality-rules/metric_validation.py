def validate_metrics(metrics: dict) -> tuple[bool, list[str]]:
    """Validate aggregated quality metrics."""

    errors = []

    total_events = metrics.get("total_events")
    valid_events = metrics.get("valid_events")
    invalid_events = metrics.get("invalid_events")
    quality_score = metrics.get("quality_score")
    invalid_event_rate = metrics.get("invalid_event_rate")

    if total_events is None or total_events < 0:
        errors.append("total_events must be greater than or equal to 0")

    if valid_events is None or valid_events < 0:
        errors.append("valid_events must be greater than or equal to 0")

    if invalid_events is None or invalid_events < 0:
        errors.append("invalid_events must be greater than or equal to 0")

    if (
        total_events is not None
        and valid_events is not None
        and invalid_events is not None
        and valid_events + invalid_events > total_events
    ):
        errors.append(
            "valid_events + invalid_events cannot exceed total_events"
        )

    if quality_score is None or not 0 <= quality_score <= 100:
        errors.append("quality_score must be between 0 and 100")

    if invalid_event_rate is None or not 0 <= invalid_event_rate <= 100:
        errors.append("invalid_event_rate must be between 0 and 100")

    return len(errors) == 0, errors