import asyncio
from connectors.ccxt_rest import CCXTRestConnector
from connectors.ws_connectors import subscribe_ir_orderbook

async def test():
    async def print_ob(data):
        print("Orderbook update:", data["bids"][:1], data["asks"][:1])

    # Test REST fallback
    rest = CCXTRestConnector("independentreserve")
    ob = await rest.get_orderbook_rest("BTC/AUD")
    print("REST snapshot:", ob["bids"][:1], ob["asks"][:1])
    await rest.close()

    # Test WS live feed
    await subscribe_ir_orderbook("BTC/AUD", print_ob)

asyncio.run(test())
