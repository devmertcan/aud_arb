import asyncio
import subprocess
from connectors.ccxt_rest import CCXTRestConnector
from connectors.ws_connectors import subscribe_ir_orderbook
from orderbook.orderbook_manager import Orderbook
from detector.arb_detector import ArbitrageDetector
from reporter.csv_reporter import TOBReporter


PAIR = "BTC/AUD"

async def test_kraken_rest():
    print("=== Step 4a: Testing Kraken REST ===")
    rest = CCXTRestConnector("kraken")
    ob = await rest.get_orderbook_rest(PAIR)
    await rest.close()
    bids, asks = ob.get("bids", []), ob.get("asks", [])
    assert bids and asks, "No bids/asks from Kraken"
    print("✅ Step 4a: Kraken REST connector working")

async def test_ir_ws():
    print("=== Step 4b: Testing Independent Reserve WS ===")
    async def on_ob(ob):
        print("✅ Step 4b: Independent Reserve WS connector working")
        raise asyncio.CancelledError()  # stop after first message
    try:
        await subscribe_ir_orderbook(PAIR, on_ob)
    except asyncio.CancelledError:
        pass

def test_orderbook_manager():
    print("=== Step 5: Testing Orderbook Manager ===")
    ob = Orderbook()
    ob.apply_snapshot([(100, 1)], [(101, 1)])
    bid = ob.best_bid()
    ask = ob.best_ask()

    # Unpack if tuple (price, size)
    if isinstance(bid, tuple):
        bid_price = bid[0]
    else:
        bid_price = bid
    if isinstance(ask, tuple):
        ask_price = ask[0]
    else:
        ask_price = ask

    assert bid_price is not None and ask_price is not None, "Orderbook did not update correctly"
    assert bid_price <= 100.0 and ask_price >= 101.0, "Best bid/ask values not as expected"
    print(f"✅ Step 5: Orderbook Manager working (bid={bid}, ask={ask})")


def test_detector():
    print("=== Step 6: Testing Arbitrage Detector ===")
    ob_a = Orderbook(); ob_b = Orderbook()
    ob_a.apply_snapshot([(100, 1)], [(102, 1)])
    ob_b.apply_snapshot([(105, 1)], [(107, 1)])
    detector = ArbitrageDetector({"A": ob_a, "B": ob_b}, threshold=0.01)
    triggered = []
    detector.report_fn = triggered.append
    detector.check_opportunity()
    assert triggered, "Detector did not fire"
    print("✅ Step 6: Arbitrage Detector working")

def test_reporter():
    print("=== Step 7: Testing Reporter ===")
    rep = TOBReporter(path="/opt/aud_arb/out/test_report.csv")
    rep.write_tob("kraken", PAIR, 100, 1, 101, 2)
    print("✅ Step 7: Reporter writing to CSV")

def test_detector_pytest():
    print("=== Step 9: Running detector unit test (pytest) ===")
    result = subprocess.run(
        ["pytest", "-q", "--disable-warnings", "tests/test_detector.py"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print("✅ Step 9: Detector unit test passed")
    else:
        print("❌ Step 9: Detector unit test failed")
        print(result.stdout, result.stderr)


async def main():
    print("=== Running project milestone checklist ===")
    await test_kraken_rest()
    await test_ir_ws()
    test_orderbook_manager()
    test_detector()
    test_reporter()
    test_detector_pytest()


if __name__ == "__main__":
    asyncio.run(main())
