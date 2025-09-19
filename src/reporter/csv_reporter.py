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
                    "spread"
                ])

    def write_tob(self, exchange: str, pair: str,
                  bid: Tuple[float, float], ask: Tuple[float, float],
                  ts_ms: int):
        spread = ask[0] - bid[0] if bid and ask else None
        with self.path.open("a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.utcfromtimestamp((ts_ms or 0)/1000).isoformat() + "Z",
                exchange, pair,
                bid[0] if bid else None, bid[1] if bid else None,
                ask[0] if ask else None, ask[1] if ask else None,
                spread
            ])
