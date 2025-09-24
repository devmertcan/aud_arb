import asyncio
import pytest
from orderbook.exchange_orderbook_manager import ExchangeOrderbookManager

@pytest.mark.asyncio
async def test_manager_queue(monkeypatch):
    # Monkeypatch CCXTRest.stream_pairs to simulate one snapshot and return immediately
    async def fake_stream_pairs(self, pairs, out_queue):
        await out_queue.put({
            "ts_ms": 123,
            "exchange": "fake",
            "pair": "BTC/AUD",
            "best_bid": 100,
            "bid_size": 1,
            "best_ask": 101,
            "ask_size": 2,
            "source": "REST"
        })
        return  # exit immediately

    monkeypatch.setattr(
        "connectors.ccxt_rest.CCXTRest.stream_pairs", fake_stream_pairs
    )

    # Create manager but don't call _start_rest (infinite loop)
    manager = ExchangeOrderbookManager(["fake"], ["BTC/AUD"], poll_interval_ms_rest=100)

    # Directly run the patched method once
    await manager._start_rest("fake", ["BTC/AUD"], asyncio.Semaphore(1))

    # Verify snapshot came through
    snap = await manager.queue.get()
    assert snap["exchange"] == "fake"
    assert snap["pair"] == "BTC/AUD"
    assert snap["best_bid"] == 100
    assert snap["best_ask"] == 101
