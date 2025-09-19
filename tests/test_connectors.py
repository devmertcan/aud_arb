import asyncio
from connectors.ccxt_rest import CCXTRestConnector

async def main():
    r = CCXTRestConnector("kraken")
    ob = await r.get_orderbook_rest("BTC/AUD")
    print("Kraken REST:", ob["bids"][:1], ob["asks"][:1])
    await r.close()

if __name__ == "__main__":
    asyncio.run(main())
