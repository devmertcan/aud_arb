import asyncio
from connectors.ccxt_rest import CCXTRestConnector
from connectors.ws_connectors import subscribe_ir_orderbook
from orderbook.orderbook_manager import Orderbook
from detector.arb_detector import ArbitrageDetector
from reporter.csv_reporter import TOBReporter


def test_kraken_rest():
    print("=== Step 4a: Testing Kraken REST ===")
    async def _inner():
        r = CCXTRestConnector("kraken")
        ob = await r.get_orderbook_rest("BTC/AUD")
        await r.close()
        assert ob["bids"] and ob["asks"], "No bids/asks from Kraken"
        print("✅ Step 4a: Kraken REST connector working")
    asyncio.run(_inner())


def test_ir_ws():
    print("=== Step 4b: Testing Independent Reserve WS ===")
    async def _inner():
        async def on_ob(msg):
            if "Event" in msg and msg["Event"] == "Subscriptions":
                print(f"[IR] Subscribed: {msg['Data']}")
                raise asyncio.CancelledError()

        try:
            await subscribe_ir_orderbook("BTC/AUD", on_ob)
        except asyncio.CancelledError:
            print("✅ Step 4b: Independent Reserve WS connector working")
    asyncio.run(_inner())


def test_orderbook_manager():
    print("=== Step 5: Testing Orderbook Manager ===")
    ob = Orderbook()
    ob.apply_snapshot([(100, 1)], [(101, 1)])
    bid, ask = ob.best_bid(), ob.best_ask()
    assert bid and ask, "Orderbook missing bid/ask"
    print(f"✅ Step 5: Orderbook Manager working (bid={bid}, ask={ask})")


def test_detector():
    print("=== Step 6: Testing Arbitrage Detector ===")
    ob_a, ob_b = Orderbook(), Orderbook()
    # Force arbitrage: A has high bid, B has cheap ask
    ob_a.apply_snapshot([(110, 1)], [(111, 1)])
    ob_b.apply_snapshot([(95, 1)], [(96, 1)])

    triggered = []
    detector = ArbitrageDetector(
        {"A": ob_a, "B": ob_b}, threshold=0.01, report_fn=triggered.append
    )
    detector.check_opportunity()

    assert triggered, "Detector did not fire"
    print("✅ Step 6: Arbitrage Detector working →", triggered[0])


def test_reporter():
    print("=== Step 7: Testing Reporter ===")
    r = TOBReporter("out/test_checklist.csv")
    r.write_tob("kraken", "BTC/AUD", 100, 1, 101, 1)
    print("✅ Step 7: Reporter writing to CSV")


async def main():
    test_kraken_rest()
    test_ir_ws()
    test_orderbook_manager()
    test_detector()
    test_reporter()


if __name__ == "__main__":
    print("=== Running project milestone checklist ===")
    asyncio.run(main())
