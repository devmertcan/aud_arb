from typing import List, Tuple, Dict, Optional, Callable, Any

class ExchangeOrderBookManager:
    """
    Exchange-agnostic in-memory order book with:
      - id map (order_id -> (side, price))
      - sequence/nonce validation
      - async resync via REST snapshot when gaps detected
      - pluggable event parser: event -> list[(action, kwargs)]
      - timestamp tracking
      - midprice calculation
    """
    def __init__(self, exchange_id: str, depth: int = 20, gap_threshold: int = 1):
        self.exchange_id = exchange_id
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.id_map: Dict[str, Tuple[str, float]] = {}

        self.depth = depth
        self.last_seq: Optional[int] = None
        self.in_sync: bool = True

        self.gap_threshold = gap_threshold
        self.missed_updates = 0
        self.last_update_ts: Optional[int] = None

        self.resync_callback: Optional[Callable[[], Any]] = None
        self.parser: Optional[Callable[[Dict], List[Tuple[str, Any]]]] = None

    async def apply_snapshot(self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]], ts: Optional[int] = None):
        self.bids = {p: v for p, v in bids}
        self.asks = {p: v for p, v in asks}
        self.id_map.clear()
        self.last_seq = None
        self.in_sync = True
        self.missed_updates = 0
        self.last_update_ts = ts

    async def check_seq(self, seq: Optional[int]) -> bool:
        if seq is None:
            return True
        if self.last_seq is None:
            self.last_seq = seq
            return True
        if seq == self.last_seq + 1:
            self.last_seq = seq
            self.missed_updates = 0
            return True

        # gap or reset
        self.missed_updates += 1
        print(f"[WARN][{self.exchange_id}] seq gap {self.missed_updates}/{self.gap_threshold} (last={self.last_seq}, now={seq})")
        self.last_seq = seq

        if self.missed_updates >= self.gap_threshold and self.resync_callback:
            print(f"[ACTION][{self.exchange_id}] resync via REST snapshot…")
            snapshot = await self.resync_callback()
            await self.apply_snapshot(snapshot["bids"], snapshot["asks"], snapshot.get("timestamp"))
            self.in_sync = True
            self.missed_updates = 0
        return False

    def update_from_event(self, event: Dict):
        if not self.parser:
            raise ValueError(f"No parser set for {self.exchange_id}")
        actions = self.parser(event)
        self.last_update_ts = event.get("Time", self.last_update_ts)
        for action, kwargs in actions:
            if action == "add":
                self._add_order(**kwargs)
            elif action == "change":
                self._change_order(**kwargs)
            elif action == "cancel":
                self._cancel_order(**kwargs)

    def _add_order(self, order_id: str, side: str, price: float, volume: float):
        book = self.bids if side == "bid" else self.asks
        book[price] = volume
        self.id_map[order_id] = (side, price)

    def _change_order(self, order_id: str, volume: float):
        if order_id not in self.id_map:
            return
        side, price = self.id_map[order_id]
        book = self.bids if side == "bid" else self.asks
        if volume == 0:
            self._cancel_order(order_id)
        else:
            book[price] = volume

    def _cancel_order(self, order_id: str):
        if order_id not in self.id_map:
            return
        side, price = self.id_map.pop(order_id)
        book = self.bids if side == "bid" else self.asks
        book.pop(price, None)

    # Convenience
    def best_bid(self):
        return max(self.bids.items(), key=lambda x: x[0], default=None)

    def best_ask(self):
        return min(self.asks.items(), key=lambda x: x[0], default=None)

    def midprice(self) -> Optional[float]:
        if not self.bids or not self.asks:
            return None
        return (self.best_bid()[0] + self.best_ask()[0]) / 2

    def top_bids(self):
        return sorted(self.bids.items(), key=lambda x: -x[0])[:self.depth]

    def top_asks(self):
        return sorted(self.asks.items(), key=lambda x: x[0])[:self.depth]

    def spread(self):
        if not self.bids or not self.asks:
            return None
        return self.best_ask()[0] - self.best_bid()[0]

    def as_dict(self):
        return {
            "bids": self.top_bids(),
            "asks": self.top_asks(),
            "midprice": self.midprice(),
            "timestamp": self.last_update_ts,
        }
