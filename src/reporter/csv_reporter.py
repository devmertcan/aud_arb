import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Iterable, Tuple

class CSVReporter:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_header()

    def _ensure_header(self):
        if not self.path.exists():
            with self.path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp_iso", "exchange", "pair",
                    "best_bid_price", "best_bid_size",
                    "best_ask_price", "best_ask_size",
                    "spread",
                    "notes"
                ])

    def write_top_of_book(self, exchange: str, pair: str, bids: Iterable[Tuple[float, float]], asks: Iterable[Tuple[float, float]], ts_ms: int, notes: str = ""):
        bids = list(bids)
        asks = list(asks)
        if not bids or not asks:
            return
        best_bid_price, best_bid_size = bids[0]
        best_ask_price, best_ask_size = asks[0]
        spread = best_ask_price - best_bid_price
        with self.path.open("a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.utcfromtimestamp((ts_ms or 0)/1000).isoformat() + "Z",
                exchange, pair,
                best_bid_price, best_bid_size,
                best_ask_price, best_ask_size,
                spread,
                notes
            ])

class TOBReporter:
    def __init__(self, path="/opt/aud_arb/out/tob_snapshots.csv"):
        self.path = path
        self.header_written = False

    def write_tob(self, exchange, pair, bid_price, bid_size, ask_price, ask_size, timestamp=None):
        import csv, os, datetime
        row = {
            "timestamp_iso": timestamp or datetime.datetime.utcnow().isoformat() + "Z",
            "exchange": exchange,
            "pair": pair,
            "best_bid_price": bid_price,
            "best_bid_size": bid_size,
            "best_ask_price": ask_price,
            "best_ask_size": ask_size,
            "spread": ask_price - bid_price,
        }
        file_exists = os.path.exists(self.path)
        with open(self.path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
