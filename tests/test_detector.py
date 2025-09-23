import asyncio
import pytest
import time
from detector.arb_detector import ArbDetector
from reporter.csv_reporter import CSVReporter
import os
import csv

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

    now_ms = int(time.time() * 1000)
    snap1 = {"ts_ms": now_ms, "exchange": "ex1", "pair": "BTC/AUD",
             "best_bid": 99, "bid_size": 1, "best_ask": 100, "ask_size": 1}
    snap2 = {"ts_ms": now_ms, "exchange": "ex2", "pair": "BTC/AUD",
             "best_bid": 105, "bid_size": 1, "best_ask": 106, "ask_size": 1}
    detector.latest[("ex1", "BTC/AUD")] = snap1
    detector.latest[("ex2", "BTC/AUD")] = snap2

    await detector._evaluate_pair("BTC/AUD")

    # Parse CSV
    with open(csv_path) as f:
        rows = list(csv.DictReader(f))
    assert len(rows) > 0
    row = rows[0]
    assert row["pair"] == "BTC/AUD"
    assert float(row["spread_aud"]) > 0
