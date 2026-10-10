
import os
from pathlib import Path


# Load simple KEY=VALUE pairs from the project .env file.
ENV_FILE = Path(__file__).resolve().parent.parent / ".env"

if ENV_FILE.exists():
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if key and key not in os.environ:
            os.environ[key] = value


def get_int(name: str, default: str) -> int:
    """Read an integer environment variable with a clear error."""
    try:
        return int(os.getenv(name, default))
    except ValueError as exc:
        raise ValueError(f"Invalid integer value for {name}") from exc


def get_bool(name: str, default: str) -> bool:
    """Read a boolean environment variable and validate its value."""
    value = os.getenv(name, default).strip().lower()

    if value in ("true", "1", "yes", "on"):
        return True
    if value in ("false", "0", "no", "off"):
        return False

    raise ValueError(f"Invalid boolean value for {name}: {value}")


APP_ENV = os.getenv("APP_ENV", "development")
ENVIRONMENT = os.getenv("ENVIRONMENT", APP_ENV)

HOST = os.getenv("HOST", "0.0.0.0")
PORT = get_int("PORT", "8000")
DEBUG = get_bool("DEBUG", "true" if ENVIRONMENT == "development" else "false")

# Support the existing CORS_ORIGINS variable and the tested legacy name.
_cors_value = os.getenv(
    "ALLOWED_ORIGINS",
    os.getenv("CORS_ORIGINS", "http://localhost:5173"),
)
CORS_ORIGINS = [
    origin.strip()
    for origin in _cors_value.split(",")
    if origin.strip()
]
ALLOWED_ORIGINS = CORS_ORIGINS

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ICEBERG_URL = os.getenv("ICEBERG_URL", "http://localhost:8181")

WEBSOCKET_UPDATE_INTERVAL_SECONDS = float(
    os.getenv("WEBSOCKET_UPDATE_INTERVAL_SECONDS", "1.0")
)

PERFORMANCE_HISTORY_SIZE = get_int("PERFORMANCE_HISTORY_SIZE", "100")
