import csv
import os
import datetime


class TOBReporter:
    def __init__(self, path="/opt/aud_arb/out/tob_snapshots.csv"):
        self.path = path
        self.header = [
            "timestamp_iso",
            "exchange",
            "pair",
            "best_bid_price",
            "best_bid_size",
            "best_ask_price",
            "best_ask_size",
            "spread",
        ]

        # Ensure output directory exists
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

        # Write header if file does not exist
        if not os.path.exists(self.path):
            with open(self.path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(self.header)

    def write_tob(
        self,
        exchange: str,
        pair: str,
        bid_price: float,
        bid_size: float,
        ask_price: float,
        ask_size: float,
        timestamp: str | None = None,
    ):
        """Write one top-of-book snapshot row."""
        ts = timestamp or datetime.datetime.utcnow().isoformat() + "Z"
        row = [
            ts,
            exchange,
            pair,
            float(bid_price),
            float(bid_size),
            float(ask_price),
            float(ask_size),
            float(ask_price) - float(bid_price),
        ]
        with open(self.path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(row)
import csv
import os
from datetime import datetime, timezone
from typing import Dict, Any

class CSVReporter:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
        self._ensure_header()

    def _ensure_header(self):
        if not os.path.exists(self.csv_path) or os.path.getsize(self.csv_path) == 0:
            with open(self.csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp_iso",
                    "exchange_buy", "exchange_sell",
                    "pair",
                    "best_ask_buy_ex", "best_bid_sell_ex",
                    "spread_aud", "spread_pct",
                    "after_fee_spread_pct",
                    "notional_aud",
                    "confidence",
                    "reason"
                ])

    def write_snapshot(self, row: Dict[str, Any]):
        # row expected fields per header above
        row_out = [
            datetime.now(timezone.utc).isoformat(),
            row.get("exchange_buy"),
            row.get("exchange_sell"),
            row.get("pair"),
            row.get("best_ask_buy_ex"),
            row.get("best_bid_sell_ex"),
            f"{row.get('spread_aud', 0):.8f}",
            f"{row.get('spread_pct', 0):.6f}",
            f"{row.get('after_fee_spread_pct', 0):.6f}",
            f"{row.get('notional_aud', 0):.2f}",
            f"{row.get('confidence', 0):.3f}",
            row.get("reason", ""),
        ]
        with open(self.csv_path, "a", newline="") as f:
            csv.writer(f).writerow(row_out)
