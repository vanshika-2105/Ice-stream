from datetime import datetime
from numbers import Real


REQUIRED_FIELDS = [
    "event_id",
    "event_type",
    "timestamp",
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "amount",
    "currency",
]

ALLOWED_EVENT_TYPES = {"checkout"}

ALLOWED_CURRENCIES = {
    "INR",
    "USD",
    "EUR",
    "GBP",
}


def _error(field: str, code: str, message: str) -> dict:
    """Create a standardized validation error."""
    return {
        "field": field,
        "code": code,
        "message": message,
    }


def validate_checkout_event(event: dict) -> dict:
    """
    Validate a checkout event against the Ice-Stream checkout schema.

    Returns:
        {
            "valid": bool,
            "event_id": str | None,
            "errors": list[dict]
        }

    The validator collects all applicable errors instead of
    stopping at the first validation failure.
    """

    errors = []

    # --------------------------------------------------
    # 0. Input type
    # --------------------------------------------------

    if not isinstance(event, dict):
        return {
            "valid": False,
            "event_id": None,
            "errors": [
                _error(
                    "event",
                    "INVALID_TYPE",
                    "Checkout event must be an object",
                )
            ],
        }

    event_id = event.get("event_id")

    # --------------------------------------------------
    # 1. Required fields
    # --------------------------------------------------

    for field in REQUIRED_FIELDS:
        if field not in event:
            errors.append(
                _error(
                    field,
                    "REQUIRED_FIELD",
                    f"{field} is required",
                )
            )

    # --------------------------------------------------
    # 2. String fields
    # --------------------------------------------------

    string_fields = [
        "event_id",
        "event_type",
        "timestamp",
        "order_id",
        "customer_id",
        "product_id",
        "currency",
    ]

    for field in string_fields:
        if field in event and not isinstance(event[field], str):
            errors.append(
                _error(
                    field,
                    "INVALID_TYPE",
                    f"{field} must be a string",
                )
            )

    # --------------------------------------------------
    # 3. Quantity validation
    # --------------------------------------------------

    if "quantity" in event:
        quantity = event["quantity"]

        if isinstance(quantity, bool) or not isinstance(quantity, int):
            errors.append(
                _error(
                    "quantity",
                    "INVALID_TYPE",
                    "Quantity must be an integer",
                )
            )
        elif quantity <= 0:
            errors.append(
                _error(
                    "quantity",
                    "NON_POSITIVE_VALUE",
                    "Quantity must be greater than 0",
                )
            )

    # --------------------------------------------------
    # 4. Amount validation
    # --------------------------------------------------

    if "amount" in event:
        amount = event["amount"]

        if isinstance(amount, bool) or not isinstance(amount, Real):
            errors.append(
                _error(
                    "amount",
                    "INVALID_TYPE",
                    "Amount must be a number",
                )
            )
        elif amount < 0:
            errors.append(
                _error(
                    "amount",
                    "NEGATIVE_VALUE",
                    "Amount cannot be negative",
                )
            )

    # --------------------------------------------------
    # 5. Event type validation
    # --------------------------------------------------

    if "event_type" in event and isinstance(event["event_type"], str):
        if event["event_type"] not in ALLOWED_EVENT_TYPES:
            errors.append(
                _error(
                    "event_type",
                    "INVALID_EVENT_TYPE",
                    "Event type must be 'checkout'",
                )
            )

    # --------------------------------------------------
    # 6. Currency validation
    # --------------------------------------------------

    if "currency" in event and isinstance(event["currency"], str):
        if event["currency"] not in ALLOWED_CURRENCIES:
            errors.append(
                _error(
                    "currency",
                    "INVALID_CURRENCY",
                    "Currency must be one of INR, USD, EUR, or GBP",
                )
            )

    # --------------------------------------------------
    # 7. Timestamp validation
    # --------------------------------------------------

    if "timestamp" in event and isinstance(event["timestamp"], str):
        timestamp = event["timestamp"]

        try:
            datetime.fromisoformat(
                timestamp.replace("Z", "+00:00")
            )
        except ValueError:
            errors.append(
                _error(
                    "timestamp",
                    "INVALID_TIMESTAMP",
                    "Timestamp must be a valid ISO-8601 datetime",
                )
            )

    # --------------------------------------------------
    # 8. Final validation result
    # --------------------------------------------------

    return {
        "valid": len(errors) == 0,
        "event_id": event_id if isinstance(event_id, str) else None,
        "errors": errors,
    }