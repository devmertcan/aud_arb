import ccxt.async_support as ccxt


class CCXTRestConnector:
    def __init__(self, exchange_id: str):
        self.exchange_id = exchange_id
        self.exchange = getattr(ccxt, exchange_id)({"enableRateLimit": True})

    async def get_orderbook_rest(self, symbol: str):
        ob = await self.exchange.fetch_order_book(symbol)
        return {
            # Only use price and volume (ignore extra fields)
            "bids": [(float(p[0]), float(p[1])) for p in ob.get("bids", [])],
            "asks": [(float(p[0]), float(p[1])) for p in ob.get("asks", [])],
            "timestamp": ob.get("timestamp"),
        }

    async def close(self):
        await self.exchange.close()
