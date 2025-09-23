import asyncio
import pytest
from orderbook.exchange_orderbook_manager import ExchangeOrderbookManager

@pytest.mark.asyncio
async def test_manager_queue(monkeypatch):
    # fake CCXTRest client to inject snapshots
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
        await asyncio.sleep(0.1)

    monkeypatch.setattr(
        "connectors.ccxt_rest.CCXTRest.stream_pairs", fake_stream_pairs
    )

    manager = ExchangeOrderbookManager(["fake"], ["BTC/AUD"], poll_interval_ms_rest=100)
    task = asyncio.create_task(manager._start_rest("fake", ["BTC/AUD"], asyncio.Semaphore(1)))
    snap = await manager.queue.get()
    assert snap["exchange"] == "fake"
    assert snap["pair"] == "BTC/AUD"
    task.cancel()
