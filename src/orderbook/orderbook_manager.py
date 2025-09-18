import heapq
from collections import defaultdict
from typing import List, Tuple, Dict, Optional

class OrderBookManager:
    def __init__(self, depth: int = 20):
        # Store as dicts: price -> volume
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.depth = depth

    def apply_snapshot(self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]):
        """Replace with a fresh snapshot"""
        self.bids = {p: v for p, v in bids}
        self.asks = {p: v for p, v in asks}

    def update_order(self, side: str, price: float, volume: float):
        """
        Apply an incremental update:
        - side: "bid" or "ask"
        - price: float
        - volume: remaining volume at that price (0 means remove)
        """
        book = self.bids if side == "bid" else self.asks
        if volume == 0:
            book.pop(price, None)
        else:
            book[price] = volume

    def best_bid(self) -> Optional[Tuple[float, float]]:
        if not self.bids:
            return None
        price = max(self.bids.keys())
        return (price, self.bids[price])

    def best_ask(self) -> Optional[Tuple[float, float]]:
        if not self.asks:
            return None
        price = min(self.asks.keys())
        return (price, self.asks[price])

    def top_bids(self) -> List[Tuple[float, float]]:
        """Return top N bids sorted high→low"""
        return heapq.nlargest(self.depth, self.bids.items(), key=lambda x: x[0])

    def top_asks(self) -> List[Tuple[float, float]]:
        """Return top N asks sorted low→high"""
        return heapq.nsmallest(self.depth, self.asks.items(), key=lambda x: x[0])

    def spread(self) -> Optional[float]:
        if not self.bids or not self.asks:
            return None
        return self.best_ask()[0] - self.best_bid()[0]

    def as_dict(self) -> Dict:
        return {
            "bids": self.top_bids(),
            "asks": self.top_asks(),
        }
