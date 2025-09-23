import asyncio
import pytest
from detector.arb_detector import ArbDetector
from reporter.csv_reporter import CSVReporter
import os

@pytest.mark.asyncio
async def test_detector_opportunity(tmp_path):
    q = asyncio.Queue()
    csv_path = tmp_path / "out.csv"
    reporter = CSVReporter(str(csv_path))

    fee_cfg = {"default_taker_pct": 0.2, "per_exchange_taker_pct": {}}
    det_cfg = {
        "min_spread_pct_after_fees": 0.1,
        "min_notional_aud": 1.0,
        "max_age_ms": 10000,
    }

    detector = ArbDetector(
        queue=q,
        exchanges=["ex1", "ex2"],
        pairs=["BTC/AUD"],
        detection_cfg=det_cfg,
        fee_cfg=fee_cfg,
        reporter=reporter,
    )

    # Seed snapshots manually
    snap1 = {"ts_ms": 1, "exchange": "ex1", "pair": "BTC/AUD",
             "best_bid": 99, "bid_size": 1, "best_ask": 100, "ask_size": 1}
    snap2 = {"ts_ms": 2, "exchange": "ex2", "pair": "BTC/AUD",
             "best_bid": 105, "bid_size": 1, "best_ask": 106, "ask_size": 1}
    detector.latest[("ex1", "BTC/AUD")] = snap1
    detector.latest[("ex2", "BTC/AUD")] = snap2

    # Trigger evaluation and yield back to event loop
    await detector._evaluate_pair("BTC/AUD")
    await asyncio.sleep(0.05)  # give file I/O a tick

    assert os.path.exists(csv_path)
    content = csv_path.read_text()
    # It should now contain header + at least one row
    assert "BTC/AUD" in content or "ex1" in content
