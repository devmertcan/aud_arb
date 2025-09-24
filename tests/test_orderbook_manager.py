import asyncio
import pytest
from orderbook.exchange_orderbook_manager import ExchangeOrderbookManager

@pytest.mark.asyncio
async def test_manager_queue(monkeypatch):
    # Monkeypatch CCXTRest.stream_pairs to simulate one snapshot and return immediately
    async def fake_stream_pairs(self, pairs, out_queue):
        await out_queue.put({
            "ts_ms": 123,
            "exchange": "kraken",
            "pair": "BTC/AUD",
            "best_bid": 100,
            "bid_size": 1,
            "best_ask": 101,
            "ask_size": 2,
            "source": "REST"
        })
        return

    monkeypatch.setattr(
        "connectors.ccxt_rest.CCXTRest.stream_pairs", fake_stream_pairs
    )

    manager = ExchangeOrderbookManager(["kraken"], ["BTC/AUD"], poll_interval_ms_rest=100)

    # Call once, then exit
    await manager._start_rest("kraken", ["BTC/AUD"], asyncio.Semaphore(1))

    snap = await manager.queue.get()
    assert snap["exchange"] == "kraken"
    assert snap["pair"] == "BTC/AUD"
    assert snap["best_bid"] == 100
    assert snap["best_ask"] == 101
