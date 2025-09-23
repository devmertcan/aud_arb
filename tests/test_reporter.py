import os
import tempfile
from reporter.csv_reporter import CSVReporter

def test_csv_reporter_writes(tmp_path):
    csv_path = tmp_path / "snap.csv"
    rep = CSVReporter(str(csv_path))
    row = {
        "exchange_buy": "kraken",
        "exchange_sell": "okx",
        "pair": "BTC/AUD",
        "best_ask_buy_ex": 100.0,
        "best_bid_sell_ex": 101.0,
        "spread_aud": 1.0,
        "spread_pct": 1.0,
        "after_fee_spread_pct": 0.5,
        "notional_aud": 200.0,
        "confidence": 0.9,
        "reason": "OK"
    }
    rep.write_snapshot(row)
    assert os.path.exists(csv_path)
    content = csv_path.read_text()
    assert "BTC/AUD" in content
    assert "kraken" in content
    assert "okx" in content
