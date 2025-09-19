class ArbitrageDetector:
    def __init__(self, orderbooks: dict, threshold=0.007):
        self.orderbooks = orderbooks
        self.threshold = threshold  # e.g. 0.7%

    def check_opportunity(self):
        ex_names = list(self.orderbooks.keys())
        for i in range(len(ex_names)):
            for j in range(i + 1, len(ex_names)):
                ex_a = ex_names[i]
                ex_b = ex_names[j]
                ob_a = self.orderbooks[ex_a]
                ob_b = self.orderbooks[ex_b]

                bid_a = ob_a.best_bid()
                ask_b = ob_b.best_ask()
                if bid_a and ask_b:
                    spread = bid_a[0] - ask_b[0]
                    if spread > ask_b[0] * self.threshold:
                        print(
                            f"[OPPORTUNITY] Buy {ex_b} @ {ask_b[0]} / "
                            f"Sell {ex_a} @ {bid_a[0]} | Spread {spread:.2f}"
                        )
