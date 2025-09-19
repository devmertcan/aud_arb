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
