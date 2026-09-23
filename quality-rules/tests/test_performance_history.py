from pipeline_metrics import PerformanceSnapshot
from performance_history import PerformanceHistory


def create_snapshot(
    timestamp: str,
    throughput: float,
    latency: float,
) -> PerformanceSnapshot:
    return PerformanceSnapshot(
        timestamp=timestamp,
        events_received=100,
        events_processed=100,
        throughput_eps=throughput,
        average_latency_ms=latency,
        p50_latency_ms=latency,
        p95_latency_ms=latency,
        p99_latency_ms=latency,
        dlq_rate=1.0,
        status="HEALTHY",
    )


def test_history_add_snapshot():
    history = PerformanceHistory()

    snapshot = create_snapshot(
        "2026-09-20T13:00:00+00:00",
        10.0,
        120.0,
    )

    history.add_snapshot(snapshot)

    assert history.get_size() == 1
    assert history.get_latest() == snapshot


def test_history_preserves_order():
    history = PerformanceHistory()

    first = create_snapshot(
        "2026-09-20T13:00:00+00:00",
        10.0,
        120.0,
    )

    second = create_snapshot(
        "2026-09-20T13:01:00+00:00",
        25.0,
        180.0,
    )

    history.add_snapshot(first)
    history.add_snapshot(second)

    snapshots = history.get_history()

    assert snapshots[0] == first
    assert snapshots[1] == second


def test_history_empty():
    history = PerformanceHistory()

    assert history.get_size() == 0
    assert history.get_latest() is None
    assert history.get_history() == []


def test_history_max_size():
    history = PerformanceHistory(max_size=2)

    first = create_snapshot(
        "2026-09-20T13:00:00+00:00",
        10.0,
        120.0,
    )

    second = create_snapshot(
        "2026-09-20T13:01:00+00:00",
        25.0,
        180.0,
    )

    third = create_snapshot(
        "2026-09-20T13:02:00+00:00",
        50.0,
        420.0,
    )

    history.add_snapshot(first)
    history.add_snapshot(second)
    history.add_snapshot(third)

    snapshots = history.get_history()

    assert len(snapshots) == 2
    assert snapshots[0] == second
    assert snapshots[1] == third


def test_history_clear():
    history = PerformanceHistory()

    snapshot = create_snapshot(
        "2026-09-20T13:00:00+00:00",
        10.0,
        120.0,
    )

    history.add_snapshot(snapshot)
    history.clear()

    assert history.get_size() == 0
    assert history.get_latest() is None