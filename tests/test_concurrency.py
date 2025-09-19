import asyncio
import datetime
from connectors.ccxt_rest import CCXTRestConnector
from connectors.ws_connectors import subscribe_ir_orderbook
from orderbook.orderbook_manager import Orderbook
from detector.arb_detector import ArbitrageDetector
from reporter.csv_reporter import TOBReporter

PAIR = "BTC/AUD"


def now():
    return datetime.datetime.utcnow().isoformat()


async def demo_run(duration=120):
    rest = CCXTRestConnector("kraken")
    tob_reporter = TOBReporter(path="/opt/aud_arb/out/test_concurrency.csv")
    orderbooks = {"independentreserve": Orderbook(), "kraken": Orderbook()}
    detector = ArbitrageDetector(orderbooks, threshold=0.007)

    async def ir_updater():
        async def on_ob(ob):
            bids, asks = ob.get("bids", []), ob.get("asks", [])
            if bids and asks:
                best_bid, best_ask = bids[0], asks[0]
                orderbooks["independentreserve"].apply_snapshot(bids, asks)
                tob_reporter.write_tob("independentreserve", PAIR, best_bid[0], best_bid[1], best_ask[0], best_ask[1])
                print(f"[{now()}] [IR WS] bid {best_bid} | ask {best_ask}")
        await subscribe_ir_orderbook(PAIR, on_ob)

    async def kraken_updater():
        while True:
            ob = await rest.get_orderbook_rest(PAIR)
            bids, asks = ob.get("bids", []), ob.get("asks", [])
            if bids and asks:
                best_bid, best_ask = bids[0], asks[0]
                orderbooks["kraken"].apply_snapshot(bids, asks)
                tob_reporter.write_tob("kraken", PAIR, best_bid[0], best_bid[1], best_ask[0], best_ask[1])
                print(f"[{now()}] [KRAKEN SNAPSHOT] bid {best_bid} | ask {best_ask}")
            await asyncio.sleep(10)

    async def detector_loop():
        while True:
            print(f"[{now()}] [DETECTOR] checking arbitrage...")
            detector.check_opportunity()
            await asyncio.sleep(5)

    tasks = [
        asyncio.create_task(ir_updater()),
        asyncio.create_task(kraken_updater()),
        asyncio.create_task(detector_loop())
    ]

    try:
        await asyncio.sleep(duration)
    finally:
        for t in tasks:
            t.cancel()
        await rest.close()
        print(f"\n✅ Demo run finished after {duration}s. Check /opt/aud_arb/out/test_concurrency.csv for results.")


if __name__ == "__main__":
    asyncio.run(demo_run())
