from utils.logging_config import setup_logger
logger = setup_logger()

class ArbitrageDetector:
    def __init__(self, orderbooks: dict, threshold=0.007, report_fn=None):
        """
        :param orderbooks: dict of {exchange_name: Orderbook instance}
        :param threshold: minimum relative spread (e.g. 0.007 = 0.7%)
        :param report_fn: callback for reporting opportunities (default=logger)
        """
        self.orderbooks = orderbooks
        self.threshold = threshold
        self.report_fn = report_fn or (lambda msg: logger.info(msg))

    def report(self, msg: str):
        """Send message to callback or logger"""
        self.report_fn(msg)

    def check_opportunity(self):
        """Compare best bid/ask across exchanges and report arbitrage"""
        ex_names = list(self.orderbooks.keys())
        for i in range(len(ex_names)):
            for j in range(i + 1, len(ex_names)):
                ex_a, ex_b = ex_names[i], ex_names[j]
                ob_a, ob_b = self.orderbooks[ex_a], self.orderbooks[ex_b]

                bid_a, ask_a = ob_a.best_bid(), ob_a.best_ask()
                bid_b, ask_b = ob_b.best_bid(), ob_b.best_ask()

                # Case 1: Buy from B, sell to A
                if bid_a and ask_b:
                    spread = bid_a[0] - ask_b[0]
                    if spread > ask_b[0] * self.threshold:
                        self.report(
                            f"[OPPORTUNITY] Buy {ex_b} @ {ask_b[0]} / Sell {ex_a} @ {bid_a[0]} | Spread {spread:.2f}"
                        )

                # Case 2: Buy from A, sell to B
                if bid_b and ask_a:
                    spread = bid_b[0] - ask_a[0]
                    if spread > ask_a[0] * self.threshold:
                        self.report(
                            f"[OPPORTUNITY] Buy {ex_a} @ {ask_a[0]} / Sell {ex_b} @ {bid_b[0]} | Spread {spread:.2f}"
                        )
