from live_aggregator import LiveAggregator


def test_websocket_update_rate():
    aggregator = LiveAggregator()

    aggregator.record_websocket_update()
    aggregator.record_websocket_update()
    aggregator.record_websocket_update()

    assert aggregator.websocket_updates == 3
    assert aggregator.get_websocket_update_rate(3) == 1.0


def test_websocket_update_rate_zero_elapsed():
    aggregator = LiveAggregator()

    aggregator.record_websocket_update()

    assert aggregator.get_websocket_update_rate(0) == 0.0


def test_websocket_updates_reset():
    aggregator = LiveAggregator()

    aggregator.record_websocket_update()

    aggregator.reset()

    assert aggregator.websocket_updates == 0
    assert aggregator.last_websocket_update_time is None