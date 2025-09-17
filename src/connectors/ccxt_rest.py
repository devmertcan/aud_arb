import ccxt.async_support as ccxt

class CCXTRestConnector:
    def __init__(self, exchange_id: str):
        self.exchange_id = exchange_id
        self.exchange = getattr(ccxt, exchange_id)({"enableRateLimit": True})

    async def get_orderbook_rest(self, symbol: str, limit: int = 20):
        ob = await self.exchange.fetch_order_book(symbol, limit=limit)
        return {
            "bids": ob["bids"],
            "asks": ob["asks"],
            "timestamp": ob["timestamp"],
        }

    async def close(self):
        await self.exchange.close()
