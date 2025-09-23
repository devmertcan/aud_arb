import asyncio
import pytest
from connectors.ccxt_rest import CCXTRest

@pytest.mark.asyncio
async def test_fetch_ticker_best(monkeypatch):
    ex = CCXTRest("kraken", poll_interval_ms=500)

    async def fake_order_book(symbol, limit=5):
        return {"bids": [[100, 1.0]], "asks": [[101, 2.0]]}

    monkeypatch.setattr(ex.exchange, "fetch_order_book", fake_order_book)

    result = await ex.fetch_ticker_best("BTC/AUD")
    assert result is not None
    best_bid, bid_size, best_ask, ask_size = result
    assert best_bid == 100
    assert best_ask == 101
    assert bid_size == 1.0
    assert ask_size == 2.0
    await ex.close()
