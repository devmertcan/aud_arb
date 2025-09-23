import asyncio
import time
from typing import Dict, Any, List, Optional, Tuple

import ccxt.async_support as ccxt

# Some exchanges don’t have official CCXT ids in the exact names used in config.
# Map aliases here if needed.
EXCHANGE_ID_ALIASES = {
    "independentreserve": "independentreserve",
    "kraken": "kraken",
    "okx": "okx",
    "btcmarkets": "btcmarkets",
    "coinspot": "coinspot",     # supported in ccxt (read-only public) on many installs; fallback gracefully if not available
    "swyftx": "swyftx",         # same note as above
}

class CCXTRest:
    def __init__(self, exchange_id: str, poll_interval_ms: int = 1000):
        ccxt_id = EXCHANGE_ID_ALIASES.get(exchange_id, exchange_id)
        if not hasattr(ccxt, ccxt_id):
            raise ValueError(f"Exchange {exchange_id} not supported by ccxt in this build.")
        self.exchange = getattr(ccxt, ccxt_id)({
            "enableRateLimit": True,
        })
        self.exchange_id = exchange_id
        self.poll_interval = poll_interval_ms / 1000.0
        self._closed = False

    async def close(self):
        self._closed = True
        try:
            await self.exchange.close()
        except Exception:
            pass

    async def fetch_ticker_best(self, symbol: str) -> Optional[Tuple[float, float, float, float]]:
        """
        Returns (best_bid, bid_size, best_ask, ask_size) or None.
        Uses fetch_order_book if fetch_ticker lacks depth.
        """
        try:
            ob = await self.exchange.fetch_order_book(symbol, limit=5)
            bids = ob.get("bids") or []
            asks = ob.get("asks") or []
            if not bids or not asks:
                return None
            best_bid, bid_size = bids[0][0], bids[0][1]
            best_ask, ask_size = asks[0][0], asks[0][1]
            return best_bid, bid_size, best_ask, ask_size
        except Exception:
            return None

    async def stream_pairs(self, pairs: List[str], out_queue: asyncio.Queue):
        """
        Polls order books for given pairs and pushes snapshots to out_queue.
        Snapshot schema:
        {
          "ts_ms": int,
          "exchange": str,
          "pair": str,
          "best_bid": float,
          "bid_size": float,
          "best_ask": float,
          "ask_size": float,
          "source": "REST"
        }
        """
        try:
            while not self._closed:
                start = time.time()
                for pair in pairs:
                    snap = await self.fetch_ticker_best(pair)
                    if snap:
                        best_bid, bid_size, best_ask, ask_size = snap
                        await out_queue.put({
                            "ts_ms": int(time.time() * 1000),
                            "exchange": self.exchange_id,
                            "pair": pair,
                            "best_bid": best_bid,
                            "bid_size": bid_size,
                            "best_ask": best_ask,
                            "ask_size": ask_size,
                            "source": "REST",
                        })
                # throttle
                elapsed = time.time() - start
                sleep_for = max(0.0, self.poll_interval - elapsed)
                await asyncio.sleep(sleep_for)
        finally:
            await self.close()
