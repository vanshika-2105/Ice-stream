import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from alert_server.logging_config import (
    ObservabilityEventStore,
    get_recent_logs,
    logger,
    set_request_id,
)


def test_request_id_is_preserved_in_log():
    set_request_id("test-request-001")

    logger.info(
        "test correlation log",
        extra={"component": "test"},
    )

    logs = get_recent_logs(1)

    assert logs[-1]["request_id"] == "test-request-001"
    assert logs[-1]["component"] == "test"


def test_log_contains_standard_level():
    logger.warning(
        "test warning",
        extra={"component": "test"},
    )

    logs = get_recent_logs(1)

    assert logs[-1]["level"] == "WARNING"


def test_log_contains_timestamp_and_message():
    logger.info(
        "test observability message",
        extra={"component": "test"},
    )

    logs = get_recent_logs(1)

    assert logs[-1]["timestamp"]
    assert logs[-1]["message"] == "test observability message"


def test_log_store_is_bounded_to_500_events():
    store = ObservabilityEventStore()

    for index in range(600):
        record = logger.makeRecord(
            "ice_stream",
            20,
            __file__,
            0,
            f"event-{index}",
            (),
            None,
        )

        store.emit(record)

    assert len(store.events) == 500
    assert store.events[0]["message"] == "event-100"
    assert store.events[-1]["message"] == "event-599"


def test_get_recent_logs_respects_limit():
    logger.info(
        "limit test 1",
        extra={"component": "test"},
    )
    logger.info(
        "limit test 2",
        extra={"component": "test"},
    )

    logs = get_recent_logs(1)

    assert len(logs) == 1
    assert logs[-1]["message"] == "limit test 2"


def test_log_component_is_recorded():
    logger.error(
        "test error component",
        extra={"component": "quality"},
    )

    logs = get_recent_logs(1)

    assert logs[-1]["component"] == "quality"
    assert logs[-1]["level"] == "ERROR"