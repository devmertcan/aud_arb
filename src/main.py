import asyncio
import uvloop
import datetime

from connectors.ccxt_rest import CCXTRestConnector
from connectors.ws_connectors import subscribe_ir_orderbook
from orderbook.orderbook_manager import Orderbook
from detector.arb_detector import ArbitrageDetector
from reporter.csv_reporter import TOBReporter
from utils.logging_config import setup_logger

logger = setup_logger()
PAIR = "BTC/AUD"
rest = CCXTRestConnector("kraken")
tob_reporter = TOBReporter()


def now():
    return datetime.datetime.utcnow().isoformat()


async def fetch_kraken_snapshot(symbol: str):
    return await rest.get_orderbook_rest(symbol)


async def run_detector():
    orderbooks = {"independentreserve": Orderbook(), "kraken": Orderbook()}
    detector = ArbitrageDetector(orderbooks, threshold=0.007)

    async def ir_updater():
        # 1. Seed with REST snapshot (so CSV has initial IR data)
        rest_ir = CCXTRestConnector("independentreserve")
        snapshot = await rest_ir.get_orderbook_rest(PAIR)
        bids, asks = snapshot.get("bids", []), snapshot.get("asks", [])
        if bids and asks:
            best_bid, best_ask = bids[0], asks[0]
            orderbooks["independentreserve"].apply_snapshot(bids, asks)
            tob_reporter.write_tob(
                "independentreserve", PAIR,
                best_bid[0], best_bid[1],
                best_ask[0], best_ask[1],
                timestamp=snapshot.get("timestamp"),
            )
            logger.info(f"[IR REST SNAPSHOT] bid {best_bid} | ask {best_ask}")
        await rest_ir.close()

        # 2. WebSocket live updates
        async def on_ob(ob):
            bids, asks = ob.get("bids", []), ob.get("asks", [])
            if bids and asks:
                best_bid, best_ask = bids[0], asks[0]
                orderbooks["independentreserve"].apply_snapshot(bids, asks)
                tob_reporter.write_tob(
                    "independentreserve", PAIR,
                    best_bid[0], best_bid[1],
                    best_ask[0], best_ask[1],
                    timestamp=ob.get("timestamp"),
                )
                logger.info(f"[IR WS UPDATE] bid {best_bid} | ask {best_ask}")

        await subscribe_ir_orderbook(PAIR, on_ob)

    async def kraken_updater():
        while True:
            ob = await fetch_kraken_snapshot(PAIR)
            bids, asks = ob.get("bids", []), ob.get("asks", [])
            if bids and asks:
                best_bid, best_ask = bids[0], asks[0]
                orderbooks["kraken"].apply_snapshot(bids, asks)
                tob_reporter.write_tob(
                    "kraken", PAIR,
                    best_bid[0], best_bid[1],
                    best_ask[0], best_ask[1],
                    timestamp=ob.get("timestamp"),
                )
                logger.info(f"[KRAKEN SNAPSHOT] bid {best_bid} | ask {best_ask}")
            await asyncio.sleep(10)

    async def detector_loop():
        while True:
            logger.info(f"[DETECTOR] checking arbitrage...")
            detector.check_opportunity()
            await asyncio.sleep(5)

    async def health_logger(csv_path, interval=60):
        import os
        while True:
            try:
                rows = sum(1 for _ in open(csv_path))
                logger.info(f"[HEALTH] CSV rows = {rows}")
            except FileNotFoundError:
                logger.warning("[HEALTH] No CSV file yet")
            await asyncio.sleep(interval)

    # Run all 3 concurrently
    await asyncio.gather(
        ir_updater(),
        kraken_updater(),
        detector_loop(),
        health_logger("/opt/aud_arb/out/tob_snapshots.csv"),
    )


async def main():
    await run_detector()


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
