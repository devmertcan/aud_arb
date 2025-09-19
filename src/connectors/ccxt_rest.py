import ccxt.async_support as ccxt
from typing import Dict, Any, Optional

class CCXTRestConnector:
    """Thin REST wrapper using ccxt for snapshots."""
    def __init__(self, exchange_id: str, api_key: Optional[str] = None, secret: Optional[str] = None):
        self.exchange_id = exchange_id
        klass = getattr(ccxt, exchange_id)
        params = {"enableRateLimit": True}
        if api_key and secret:
            params.update({"apiKey": api_key, "secret": secret})
        self.exchange = klass(params)

    async def get_orderbook_rest(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        ob = await self.exchange.fetch_order_book(symbol, limit=limit)

        def norm(side):
            return [
                (float(row[0]), float(row[1]))
                for row in ob.get(side, [])
                if len(row) >= 2
            ]

        return {
            "bids": norm("bids"),
            "asks": norm("asks"),
            "timestamp": ob.get("timestamp"),
        }

    async def close(self) -> None:
        await self.exchange.close()
