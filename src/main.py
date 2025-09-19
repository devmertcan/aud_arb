import asyncio
import os

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass

from connectors.ccxt_rest import CCXTRestConnector
from connectors.ws_connectors import subscribe_ir_orderbook
from detector.arb_detector import ArbitrageDetector
from reporter.csv_reporter import CSVReporter, TOBReporter


PAIR = os.environ.get("PAIR", "BTC/AUD")
CSV_PATH = os.environ.get("CSV_PATH", "/opt/aud_arb/out/arb_opps.csv")
TOB_PATH = os.environ.get("TOB_PATH", "/opt/aud_arb/out/tob_snapshots.csv")


async def fetch_kraken_snapshot(symbol: str):
    rest = CCXTRestConnector("kraken")
    ob = await rest.get_orderbook_rest(symbol)
    await rest.close()
    return ob


async def run_detector():
    detector = ArbitrageDetector(min_profit_pct=0.7, fees_pct=0.2)
    reporter = CSVReporter(CSV_PATH)
    tob_reporter = TOBReporter(TOB_PATH)

    latest_ir = {"bids": [], "asks": []}
    latest_kraken = {"bids": [], "asks": []}

    async def on_ir_update(ob):
        nonlocal latest_ir, latest_kraken
        latest_ir = ob
        bids, asks = ob.get("bids", []), ob.get("asks", [])
        if bids and asks:
            tob_reporter.write_tob("independentreserve", PAIR, bids[0], asks[0], ob.get("timestamp"))
        if latest_kraken["bids"] and latest_kraken["asks"]:
            opp = detector.check_opportunity(latest_ir, latest_kraken,
                                             "independentreserve", "kraken", PAIR)
            if opp:
                print("ARB OPPORTUNITY:", opp)
                reporter.write_top_of_book(
                    "arb", PAIR,
                    [(opp["buy_price"], 1)], [(opp["sell_price"], 1)],
                    int(opp["timestamp"] * 1000),
                    notes=f"{opp['buy_exchange']} -> {opp['sell_exchange']} spread {opp['net_pct']}%"
                )

    # Kick off Kraken snapshot updater (REST every 10s for demo)
    async def kraken_updater():
        nonlocal latest_kraken
        while True:
            latest_kraken = await fetch_kraken_snapshot(PAIR)
            bids, asks = latest_kraken.get("bids", []), latest_kraken.get("asks", [])
            if bids and asks:
                print(f"[KRAKEN SNAPSHOT] bid {bids[0]} | ask {asks[0]} | spread {asks[0][0]-bids[0][0]:.2f}")
                tob_reporter.write_tob("kraken", PAIR, bids[0], asks[0], latest_kraken.get("timestamp"))
            await asyncio.sleep(10)

    await asyncio.gather(
        subscribe_ir_orderbook(PAIR, on_ir_update),
        kraken_updater()
    )


async def main():
    await run_detector()


if __name__ == "__main__":
    asyncio.run(main())
