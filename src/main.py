import asyncio
from connectors.ccxt_rest import CCXTRestConnector

async def test():
    rest = CCXTRestConnector("independentreserve")
    ob = await rest.get_orderbook_rest("BTC/AUD")
    print("REST snapshot:", ob["bids"][:1], ob["asks"][:1])
    await rest.close()

asyncio.run(test())
