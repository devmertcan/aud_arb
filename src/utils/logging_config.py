# src/utils/logging_config.py
import logging
import os

LOG_DIR = "/opt/aud_arb/logs"
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "bot.log")

def setup_logger(name="aud_arb"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers if called multiple times
    if not logger.handlers:
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
        ch.setFormatter(ch_formatter)

        # File handler
        fh = logging.FileHandler(LOG_FILE)
        fh.setLevel(logging.INFO)
        fh_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
        fh.setFormatter(fh_formatter)

        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger
