import time
from typing import Dict, Any, Optional


class ArbitrageDetector:
    def __init__(self, min_profit_pct: float = 0.7, fees_pct: float = 0.2):
        """
        :param min_profit_pct: Minimum spread required after fees (percent, e.g. 0.7 means 0.7%)
        :param fees_pct: Estimated combined fees for buy+sell (percent)
        """
        self.min_profit_pct = min_profit_pct / 100
        self.fees_pct = fees_pct / 100

    def check_opportunity(
        self,
        ob_a: Dict[str, Any],
        ob_b: Dict[str, Any],
        exchange_a: str,
        exchange_b: str,
        pair: str,
    ) -> Optional[Dict[str, Any]]:
        """Check for arbitrage between two exchanges."""
        if not ob_a.get("bids") or not ob_a.get("asks"):
            return None
        if not ob_b.get("bids") or not ob_b.get("asks"):
            return None

        best_bid_a = ob_a["bids"][0]
        best_ask_a = ob_a["asks"][0]
        best_bid_b = ob_b["bids"][0]
        best_ask_b = ob_b["asks"][0]

        # Case 1: Buy B, Sell A
        if best_bid_a[0] > best_ask_b[0]:
            spread = best_bid_a[0] - best_ask_b[0]
            spread_pct = spread / best_ask_b[0]
            net_pct = spread_pct - self.fees_pct
            if net_pct >= self.min_profit_pct:
                return {
                    "pair": pair,
                    "buy_exchange": exchange_b,
                    "sell_exchange": exchange_a,
                    "buy_price": best_ask_b[0],
                    "sell_price": best_bid_a[0],
                    "spread_pct": round(spread_pct * 100, 3),
                    "net_pct": round(net_pct * 100, 3),
                    "timestamp": time.time(),
                }

        # Case 2: Buy A, Sell B
        if best_bid_b[0] > best_ask_a[0]:
            spread = best_bid_b[0] - best_ask_a[0]
            spread_pct = spread / best_ask_a[0]
            net_pct = spread_pct - self.fees_pct
            if net_pct >= self.min_profit_pct:
                return {
                    "pair": pair,
                    "buy_exchange": exchange_a,
                    "sell_exchange": exchange_b,
                    "buy_price": best_ask_a[0],
                    "sell_price": best_bid_b[0],
                    "spread_pct": round(spread_pct * 100, 3),
                    "net_pct": round(net_pct * 100, 3),
                    "timestamp": time.time(),
                }

        return None
