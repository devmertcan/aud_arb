import asyncio
import math
import time
from typing import Dict, Tuple, Any, List

from reporter.csv_reporter import CSVReporter

def _taker_fee_pct(exchange: str, fee_cfg: dict) -> float:
    per = fee_cfg.get("per_exchange_taker_pct", {})
    default = fee_cfg.get("default_taker_pct", 0.2)
    return float(per.get(exchange, default))

class ArbDetector:
    """
    Consumes best bid/ask snapshots from a queue, keeps the latest book per (exchange, pair),
    and compares across exchanges to find after-fee spreads above threshold.
    """

    def __init__(
        self,
        queue: asyncio.Queue,
        exchanges: List[str],
        pairs: List[str],
        detection_cfg: dict,
        fee_cfg: dict,
        reporter: CSVReporter,
    ):
        self.q = queue
        self.exchanges = exchanges
        self.pairs = pairs
        self.det_cfg = detection_cfg
        self.fee_cfg = fee_cfg
        self.reporter = reporter

        self.max_age_ms = int(self.det_cfg.get("max_age_ms", 1500))
        self.min_spread_after_fee = float(self.det_cfg.get("min_spread_pct_after_fees", 0.7))
        self.min_notional_aud = float(self.det_cfg.get("min_notional_aud", 200))
        self.conf_min = float(self.det_cfg.get("confidence_min", 0.0))

        # latest[(exchange, pair)] = {"ts_ms": int, "best_bid": float, "bid_size": float, "best_ask": float, "ask_size": float}
        self.latest: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def _confidence(self, a: Dict[str, Any], b: Dict[str, Any]) -> float:
        # lightweight heuristic: larger of top sizes & freshness
        if not a or not b:
            return 0.0
        age_penalty = 0.0
        now = int(time.time() * 1000)
        for ob in (a, b):
            if now - ob["ts_ms"] > self.max_age_ms:
                age_penalty += 0.25
        depth = min(a.get("bid_size", 0.0) + a.get("ask_size", 0.0),
                    b.get("bid_size", 0.0) + b.get("ask_size", 0.0))
        base = 0.5 if depth >= 0.5 else 0.25 if depth >= 0.1 else 0.1
        conf = max(0.0, min(1.0, base - age_penalty))
        return conf

    def _after_fee_spread_pct(self, buy_ex: str, sell_ex: str, best_ask: float, best_bid: float) -> float:
        if not best_ask or not best_bid or best_ask <= 0:
            return 0.0
        gross = (best_bid - best_ask) / best_ask * 100.0
        buy_fee = _taker_fee_pct(buy_ex, self.fee_cfg)
        sell_fee = _taker_fee_pct(sell_ex, self.fee_cfg)
        # very simple fee model: subtract fees
        net = gross - buy_fee - sell_fee
        return net

    async def run(self):
        while True:
            snap = await self.q.get()
            key = (snap["exchange"].lower(), snap["pair"])
            self.latest[key] = snap
            # try to evaluate the pair across all exchange combos whenever we refresh one side
            await self._evaluate_pair(snap["pair"])

    async def _evaluate_pair(self, pair: str):
        now = int(time.time() * 1000)
        # collect recent books for this pair
        books: List[Tuple[str, Dict[str, Any]]] = []
        for ex in self.exchanges:
            ob = self.latest.get((ex, pair))
            if not ob:
                continue
            if now - ob["ts_ms"] > self.max_age_ms:
                continue
            books.append((ex, ob))

        # compare all combos
        n = len(books)
        for i in range(n):
            ex_buy, ob_buy = books[i]     # we buy at best_ask on ex_buy
            for j in range(n):
                if i == j:
                    continue
                ex_sell, ob_sell = books[j]  # we sell at best_bid on ex_sell
                best_ask = ob_buy["best_ask"]
                best_bid = ob_sell["best_bid"]
                if best_ask <= 0 or best_bid <= 0:
                    continue
                spread_aud = best_bid - best_ask
                spread_pct = (spread_aud / best_ask) * 100.0
                after_fee = self._after_fee_spread_pct(ex_buy, ex_sell, best_ask, best_bid)
                # notional assumption: we can at least fill min of top sizes * price
                notional = min(ob_buy["ask_size"], ob_sell["bid_size"]) * best_ask
                conf = self._confidence(ob_buy, ob_sell)

                reason = []
                if notional < self.min_notional_aud:
                    reason.append("notional_too_small")
                if conf < self.conf_min:
                    reason.append("low_confidence")
                if after_fee < self.min_spread_after_fee:
                    reason.append("below_after_fee_threshold")

                if not reason:
                    # Opportunity passes all filters -> record it
                    self.reporter.write_snapshot({
                        "exchange_buy": ex_buy,
                        "exchange_sell": ex_sell,
                        "pair": pair,
                        "best_ask_buy_ex": best_ask,
                        "best_bid_sell_ex": best_bid,
                        "spread_aud": spread_aud,
                        "spread_pct": spread_pct,
                        "after_fee_spread_pct": after_fee,
                        "notional_aud": notional,
                        "confidence": conf,
                        "reason": "OK",
                    })
                else:
                    # Also record below threshold to help debugging/plots (optional)
                    self.reporter.write_snapshot({
                        "exchange_buy": ex_buy,
                        "exchange_sell": ex_sell,
                        "pair": pair,
                        "best_ask_buy_ex": best_ask,
                        "best_bid_sell_ex": best_bid,
                        "spread_aud": spread_aud,
                        "spread_pct": spread_pct,
                        "after_fee_spread_pct": after_fee,
                        "notional_aud": notional,
                        "confidence": conf,
                        "reason": ",".join(reason),
                    })
