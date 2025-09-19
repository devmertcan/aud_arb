import asyncio
from connectors.ccxt_rest import CCXTRestConnector
from connectors.ws_connectors import subscribe_ir_orderbook


PAIR = "BTC/AUD"


async def test_kraken_rest():
    print("=== Testing Kraken REST ===")
    r = CCXTRestConnector("kraken")
    ob = await r.get_orderbook_rest(PAIR)
    print("Kraken REST top levels:", ob["bids"][:1], ob["asks"][:1])
    await r.close()


async def test_ir_ws():
    print("=== Testing Independent Reserve WebSocket ===")

    async def on_ob(ob):
        bids, asks = ob.get("bids", []), ob.get("asks", [])
        if bids and asks:
            print("IR WS update:", bids[:1], asks[:1])
            raise SystemExit  # exit after first valid update

    try:
        await subscribe_ir_orderbook(PAIR, on_ob)
    except SystemExit:
        pass


async def main():
    await test_kraken_rest()
    await test_ir_ws()


if __name__ == "__main__":
    asyncio.run(main())
