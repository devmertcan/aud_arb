# tests/test_detector.py
from detector.arb_detector import ArbitrageDetector
from orderbook.orderbook_manager import Orderbook

def test_detector_simple_opportunity():
    # Create fake orderbooks
    ob_a = Orderbook()
    ob_b = Orderbook()
    ob_a.apply_snapshot(bids=[(100, 1)], asks=[(102, 1)])
    ob_b.apply_snapshot(bids=[(105, 1)], asks=[(107, 1)])

    orderbooks = {"A": ob_a, "B": ob_b}
    detector = ArbitrageDetector(orderbooks, threshold=0.01)  # 1% threshold

    opportunities = []

    def capture(msg):
        opportunities.append(msg)

    detector.report_fn = capture
    detector.check_opportunity()

    assert len(opportunities) > 0, "Detector should find arbitrage"
    print("✅ Detector test passed:", opportunities[0])
