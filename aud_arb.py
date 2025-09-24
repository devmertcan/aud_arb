#!/usr/bin/env python3
import asyncio, os, sys, csv, time, math, signal
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from loguru import logger
import yaml
import pandas as pd
import numpy as np

# Use CCXT's async layer for real concurrency
import ccxt.async_support as ccxt_async

# ---------------------------
# Helpers / Models
# ---------------------------

class OrderBookTOB:
    __slots__ = ("bid","ask","ts")
    def __init__(self, bid: float, ask: float, ts: float):
        self.bid, self.ask, self.ts = bid, ask, ts

def bps(x: float) -> float:
    return x * 10000.0

def pct_from_bps(xbps: float) -> float:
    return xbps / 100.0

# ---------------------------
# Exchange Client (REST)
# ---------------------------

class ExchangeClient:
    def __init__(self, ex_id: str):
        self.id = ex_id
        klass = getattr(ccxt_async, ex_id)
        # enableRateLimit is implicit for async, but we guard anyway
        self.ex = klass({"enableRateLimit": True})
        self.markets_loaded = False
        self.symbol_map = set()   # unified symbols supported

    async def load(self):
        try:
            await self.ex.load_markets()
            self.markets_loaded = True
            self.symbol_map = set(self.ex.markets.keys())
            logger.info(f"[{self.id}] markets loaded: {len(self.symbol_map)}")
        except Exception as e:
            logger.error(f"[{self.id}] load_markets error: {e}")

    async def fetch_tob(self, symbol: str, depth: int = 5) -> Optional[OrderBookTOB]:
        # If symbol not on this exchange, skip fast
        if symbol not in self.symbol_map:
            return None
        try:
            ob = await self.ex.fetch_order_book(symbol, limit=depth)
            bids = ob.get("bids", [])
            asks = ob.get("asks", [])
            if not bids or not asks:
                return None
            bid = float(bids[0][0]); ask = float(asks[0][0])
            return OrderBookTOB(bid, ask, time.time())
        except Exception as e:
            # network/timeouts happen under load; keep silent-ish
            logger.debug(f"[{self.id}] {symbol} fetch_order_book err: {e}")
            return None

    async def close(self):
        try:
            await self.ex.close()
        except Exception:
            pass

# ---------------------------
# Detector
# ---------------------------

class ArbDetector:
    def __init__(self, symbols: List[str], exchange_ids: List[str], fees_bps: Dict[str,int],
                 min_profit_bps: int, slippage_bps: int, poll_ms: int,
                 out_dir: Path, log_dir: Path):
        self.symbols = symbols
        self.exchange_ids = exchange_ids
        self.fees_bps = fees_bps
        self.min_profit_bps = min_profit_bps
        self.slippage_bps = slippage_bps
        self.poll_ms = poll_ms
        self.out_dir = out_dir
        self.log_dir = log_dir
        self.clients: Dict[str, ExchangeClient] = {}
        self.state: Dict[str, Dict[str, OrderBookTOB]] = {s: {} for s in symbols}
        self.csv_path = out_dir / "tob_snapshots.csv"

        # rolling stats
        self._opp_count = 0

    async def setup(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        if not self.csv_path.exists():
            with self.csv_path.open("w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["ts_iso","timestamp","symbol","buy_ex","buy_ask","sell_ex","sell_bid",
                            "gross_spread_bps","fees_bps","slippage_bps","net_spread_bps","meets_threshold"])
        # init logger file
        logger.add(self.log_dir / "bot.log", rotation="10 MB", retention=10)

        # init clients
        for ex in self.exchange_ids:
            self.clients[ex] = ExchangeClient(ex)
        await asyncio.gather(*[c.load() for c in self.clients.values()])

        # drop any exchange that failed to load markets
        bad = [ex for ex,c in self.clients.items() if not c.markets_loaded]
        for ex in bad:
            logger.warning(f"Removing {ex}: failed to load markets")
            del self.clients[ex]

        if not self.clients:
            raise RuntimeError("No exchanges available after load_markets()")

    async def _poll_exchange_symbol(self, ex_id: str, symbol: str):
        client = self.clients[ex_id]
        interval = max(self.poll_ms, 100) / 1000.0
        while True:
            tob = await client.fetch_tob(symbol)
            if tob:
                self.state[symbol][ex_id] = tob
            await asyncio.sleep(interval)

    async def _writer_loop(self):
        # periodically compute best cross-exchange spreads and write rows
        while True:
            now = time.time()
            for symbol in self.symbols:
                books = self.state.get(symbol, {})
                if len(books) < 2:  # need at least 2 exchanges
                    continue
                # best venues
                best_sell_ex, best_sell_bid = None, -math.inf
                best_buy_ex, best_buy_ask = None, math.inf
                for ex_id, tob in books.items():
                    if tob.bid > best_sell_bid:
                        best_sell_ex, best_sell_bid = ex_id, tob.bid
                    if tob.ask < best_buy_ask:
                        best_buy_ex, best_buy_ask = ex_id, tob.ask
                if best_buy_ex is None or best_sell_ex is None or best_buy_ex == best_sell_ex:
                    continue

                # compute spreads
                gross = (best_sell_bid - best_buy_ask) / best_buy_ask  # in fraction
                gross_bps = bps(gross)

                # simple fee model: taker on both sides
                f_buy = self.fees_bps.get(best_buy_ex, 30)
                f_sell = self.fees_bps.get(best_sell_ex, 30)
                fee_total = f_buy + f_sell

                net_bps = gross_bps - fee_total - self.slippage_bps
                meets = int(net_bps >= self.min_profit_bps)

                # write csv row
                with self.csv_path.open("a", newline="") as f:
                    w = csv.writer(f)
                    w.writerow([
                        pd.Timestamp.utcnow().isoformat(),
                        f"{now:.3f}",
                        symbol,
                        best_buy_ex, f"{best_buy_ask:.8f}",
                        best_sell_ex, f"{best_sell_bid:.8f}",
                        f"{gross_bps:.2f}",
                        fee_total,
                        self.slippage_bps,
                        f"{net_bps:.2f}",
                        meets
                    ])

                if meets:
                    self._opp_count += 1
                    logger.info(f"[OPP] {symbol} buy {best_buy_ex}@{best_buy_ask:.2f} "
                                f"-> sell {best_sell_ex}@{best_sell_bid:.2f} | "
                                f"net {net_bps:.1f} bps (gross {gross_bps:.1f} bps)")

            await asyncio.sleep(0.2)

    async def run(self):
        # spawn polling tasks for every (exchange, symbol) that exists on the exchange
        tasks = []
        for ex_id, client in self.clients.items():
            # build per-exchange symbol list (only those truly listed)
            ex_symbols = [s for s in self.symbols if s in client.symbol_map]
            if not ex_symbols:
                logger.warning(f"[{ex_id}] No requested AUD symbols available; skipping")
                continue
            for s in ex_symbols:
                tasks.append(asyncio.create_task(self._poll_exchange_symbol(ex_id, s)))
            logger.info(f"[{ex_id}] polling {len(ex_symbols)} symbols")

        if not tasks:
            raise RuntimeError("No symbols to poll across all exchanges")

        writer = asyncio.create_task(self._writer_loop())

        stop = asyncio.Future()
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_running_loop().add_signal_handler(sig, lambda s=sig: stop.set_result(True))
        await stop
        [t.cancel() for t in tasks]; writer.cancel()

    async def close(self):
        await asyncio.gather(*[c.close() for c in self.clients.values()], return_exceptions=True)

# ---------------------------
# Entrypoint
# ---------------------------

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

async def main():
    cfg = load_config("config.yaml")
    out_dir = Path(cfg.get("out_dir","./out"))
    log_dir = Path(cfg.get("log_dir","./logs"))

    det = ArbDetector(
        symbols=cfg["symbols"],
        exchange_ids=cfg["exchanges"],
        fees_bps=cfg.get("fees_bps", {}),
        min_profit_bps=int(cfg.get("min_profit_bps", 70)),
        slippage_bps=int(cfg.get("slippage_bps", 10)),
        poll_ms=int(cfg.get("poll_ms", 500)),
        out_dir=out_dir,
        log_dir=log_dir
    )
    await det.setup()
    try:
        await det.run()
    finally:
        await det.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
