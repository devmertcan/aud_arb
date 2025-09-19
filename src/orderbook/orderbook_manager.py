class Orderbook:
    def __init__(self, depth=20):
        self.bids = {}
        self.asks = {}
        self.depth = depth

    def apply_snapshot(self, bids, asks):
        self.bids = {p: s for p, s in bids[:self.depth]}
        self.asks = {p: s for p, s in asks[:self.depth]}

    def best_bid(self):
        return max(self.bids.items()) if self.bids else None

    def best_ask(self):
        return min(self.asks.items()) if self.asks else None
