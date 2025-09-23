import asyncio
import logging
import os
import sys
import yaml

from reporter.csv_reporter import CSVReporter
from orderbook.exchange_orderbook_manager import ExchangeOrderbookManager
from detector.arb_detector import ArbDetector

def setup_logging(level: str, log_path: str):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout),
        ],
    )

async def main():
    with open(os.path.join(os.path.dirname(__file__), "..", "config.yaml"), "r") as f:
        cfg = yaml.safe_load(f)

    exchanges = [e.lower() for e in cfg["exchanges"]]
    pairs = cfg["pairs"]

    io = cfg["io"]
    log_cfg = cfg.get("logging", {})
    det_cfg = cfg.get("detection", {})
    fee_cfg = cfg.get("fees", {})
    sys_cfg = cfg.get("system", {})

    setup_logging(log_cfg.get("level", "INFO"), io["log_path"])
    logger = logging.getLogger("main")

    # Shared queue fed by producers
    obm = ExchangeOrderbookManager(
        exchanges=exchanges,
        pairs=pairs,
        poll_interval_ms_rest=int(det_cfg.get("poll_interval_ms_rest", 800)),
        max_concurrency=int(sys_cfg.get("max_concurrency", 200))
    )

    reporter = CSVReporter(io["csv_path"])
    detector = ArbDetector(
        queue=obm.queue,
        exchanges=exchanges,
        pairs=pairs,
        detection_cfg=det_cfg,
        fee_cfg=fee_cfg,
        reporter=reporter,
    )

    logger.info("Starting producers and detector…")
    await obm.start()

    # Run detector forever
    await detector.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
