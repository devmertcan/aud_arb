import asyncio
import os
from connectors.ccxt_rest import CCXTRestConnector
from connectors.ws_connectors import subscribe_ir_orderbook
from orderbook.orderbook_manager import Orderbook
from detector.arb_detector import ArbitrageDetector
from reporter.csv_reporter import TOBReporter


PAIR = "BTC/AUD"
CHECK_CSV_PATH = "/opt/aud_arb/out/tob_snapshots.csv"


async def check_connectors():
    try:
        # Kraken REST
        kraken = CCXTRestConnector("kraken")
        ob = await kraken.get_orderbook_rest(PAIR)
        await kraken.close()
        if ob["bids"] and ob["asks"]:
            print("✅ Step 4a: Kraken REST connector working")
        else:
            print("❌ Step 4a: Kraken REST returned empty orderbook")

        # IR WS
        got_update = False

        async def on_ob(ob):
            nonlocal got_update
            if ob.get("bids") or ob.get("asks"):
                got_update = True
                raise SystemExit  # stop after first event

        try:
            await subscribe_ir_orderbook(PAIR, on_ob)
        except SystemExit:
            pass

        if got_update:
            print("✅ Step 4b: Independent Reserve WS connector working")
        else:
            print("❌ Step 4b: No IR WS updates received")

    except Exception as e:
        print("❌ Step 4: Connector check failed:", e)


def check_orderbook_manager():
    try:
        ob = Orderbook()
        ob.apply_snapshot([(100, 1)], [(101, 1)])
        if ob.best_bid() and ob.best_ask():
            print("✅ Step 5: Orderbook Manager working")
        else:
            print("❌ Step 5: Orderbook Manager did not return best bid/ask")
    except Exception as e:
        print("❌ Step 5: Orderbook Manager check failed:", e)


def check_detector():
    try:
        oba = Orderbook()
        obb = Orderbook()
        oba.apply_snapshot([(105, 1)], [(106, 1)])
        obb.apply_snapshot([(100, 1)], [(101, 1)])
        detector = ArbitrageDetector({"A": oba, "B": obb}, threshold=0.01)
        # This should trigger an opportunity
        detector.check_opportunity()
        print("✅ Step 6: Arbitrage Detector runs without error")
    except Exception as e:
        print("❌ Step 6: Arbitrage Detector check failed:", e)


def check_reporter():
    try:
        reporter = TOBReporter(path=CHECK_CSV_PATH)
        reporter.write_tob("test", PAIR, 100, 1, 101, 1)
        if os.path.exists(CHECK_CSV_PATH):
            print("✅ Step 7: Reporter writing to CSV")
        else:
            print("❌ Step 7: Reporter did not create CSV")
    except Exception as e:
        print("❌ Step 7: Reporter check failed:", e)


async def main():
    print("=== Running project milestone checklist ===")
    await check_connectors()
    check_orderbook_manager()
    check_detector()
    check_reporter()


if __name__ == "__main__":
    asyncio.run(main())
