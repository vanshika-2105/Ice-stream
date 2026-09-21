import os


def get_int_env(name: str, default: int) -> int:
    """Read an integer environment variable safely."""
    value = os.getenv(name)

    if value is None:
        return default

    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(
            f"{name} must be a valid integer"
        ) from exc


def get_bool_env(name: str, default: bool) -> bool:
    """Read a boolean environment variable safely."""
    value = os.getenv(name)

    if value is None:
        return default

    normalized = value.strip().lower()

    if normalized in {"true", "1", "yes", "on"}:
        return True

    if normalized in {"false", "0", "no", "off"}:
        return False

    raise ValueError(
        f"{name} must be a boolean value"
    )


def get_list_env(name: str, default: list[str]) -> list[str]:
    """Read a comma-separated environment variable."""
    value = os.getenv(name)

    if value is None:
        return default

    return [
        item.strip()
        for item in value.split(",")
        if item.strip()
    ]


# --------------------------------------------------
# Server configuration
# --------------------------------------------------

HOST = os.getenv("HOST", "0.0.0.0")

PORT = get_int_env(
    "PORT",
    8000,
)


# --------------------------------------------------
# Environment configuration
# --------------------------------------------------

ENVIRONMENT = os.getenv(
    "ENVIRONMENT",
    "development",
)

DEBUG = get_bool_env(
    "DEBUG",
    ENVIRONMENT == "development",
)


# --------------------------------------------------
# CORS configuration
# --------------------------------------------------

ALLOWED_ORIGINS = get_list_env(
    "ALLOWED_ORIGINS",
    ["http://localhost:5173"],
)


# --------------------------------------------------
# Streaming configuration
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

ICEBERG_URL = os.getenv(
    "ICEBERG_URL",
    "http://localhost:8181",
)


# --------------------------------------------------
# WebSocket configuration
# --------------------------------------------------

WEBSOCKET_UPDATE_INTERVAL_SECONDS = get_int_env(
    "WEBSOCKET_UPDATE_INTERVAL_SECONDS",
    1,
)


# --------------------------------------------------
# Performance history
# --------------------------------------------------

PERFORMANCE_HISTORY_SIZE = get_int_env(
    "PERFORMANCE_HISTORY_SIZE",
    100,
)