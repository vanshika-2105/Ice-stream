REQUIRED_FIELDS = [
    "event_id",
    "timestamp",
    "user_id",
    "amount",
    "currency",
]


def validate_event(event: dict) -> list[str]:
    """Validate a checkout event and return any quality errors."""

    errors = []

    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] in (None, ""):
            errors.append(f"{field}_required")

    # Validate amount
    if "amount" in event and event["amount"] is not None:
        if not isinstance(event["amount"], (int, float)):
            errors.append("amount_must_be_numeric")
        elif event["amount"] < 0:
            errors.append("amount_must_be_non_negative")

    # Validate currency
    if "currency" in event and event["currency"] is not None:
        if not isinstance(event["currency"], str):
            errors.append("currency_must_be_string")
        elif len(event["currency"]) != 3:
            errors.append("currency_must_be_3_characters")

    return errors