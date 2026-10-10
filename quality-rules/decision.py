from validator import validate_checkout_event


def make_quality_decision(event: dict) -> dict:
    """
    Run the checkout event through the quality engine.

    Returns a standardized quality decision that can be
    consumed by the streaming pipeline.
    """

    validation = validate_checkout_event(event)

    if validation["valid"]:
        return {
            "event_id": validation["event_id"],
            "quality_status": "VALID",
            "quality_errors": [],
        }

    return {
        "event_id": validation["event_id"],
        "quality_status": "INVALID",
        "quality_errors": validation["errors"],
    }