import asyncio
import uvloop
from connectors.ccxt_rest import CCXTRestConnector
from connectors.ws_connectors import subscribe_ir_orderbook
from orderbook.orderbook_manager import Orderbook
from detector.arb_detector import ArbitrageDetector
from reporter.csv_reporter import TOBReporter


PAIR = "BTC/AUD"
rest = CCXTRestConnector("kraken")
tob_reporter = TOBReporter()


async def fetch_kraken_snapshot(symbol: str):
    return await rest.get_orderbook_rest(symbol)


async def run_detector():
    orderbooks = {
        "independentreserve": Orderbook(),
        "kraken": Orderbook(),
    }
    detector = ArbitrageDetector(orderbooks, threshold=0.007)

    async def ir_updater():
        async def on_ob(ob):
            bids, asks = ob.get("bids", []), ob.get("asks", [])
            if bids and asks:
                best_bid = bids[0]
                best_ask = asks[0]
                orderbooks["independentreserve"].apply_snapshot(bids, asks)
                tob_reporter.write_tob(
                    "independentreserve", PAIR,
                    best_bid[0], best_bid[1],
                    best_ask[0], best_ask[1],
                    timestamp=ob.get("timestamp"),
                )
        await subscribe_ir_orderbook(PAIR, on_ob)

    async def kraken_updater():
        while True:
            ob = await fetch_kraken_snapshot(PAIR)
            bids, asks = ob.get("bids", []), ob.get("asks", [])
            if bids and asks:
                best_bid = bids[0]
                best_ask = asks[0]
                orderbooks["kraken"].apply_snapshot(bids, asks)
                tob_reporter.write_tob(
                    "kraken", PAIR,
                    best_bid[0], best_bid[1],
                    best_ask[0], best_ask[1],
                    timestamp=ob.get("timestamp"),
                )
                print(f"[KRAKEN SNAPSHOT] bid {best_bid} | ask {best_ask}")
            await asyncio.sleep(10)

    async def detector_loop():
        while True:
            detector.check_opportunity()
            await asyncio.sleep(5)

    await asyncio.gather(ir_updater(), kraken_updater(), detector_loop())


async def main():
    await run_detector()


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
