#!/usr/bin/env python3
import asyncio, os, csv, time, math, signal
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from loguru import logger
import yaml
import pandas as pd
from dotenv import load_dotenv

# CCXT async
import ccxt.async_support as ccxt_async

# Adapters
from exchanges.swyftx_adapter import SwyftxExchangeClient
from exchanges.okx_ws_adapter import OkxWSExchangeClient, OKX_PUBLIC_WS


# ---------- Utils ----------
class OrderBookTOB:
    """
    Top-of-book snapshot with optional sizes.
    bid/sell side size is base-amount at that level.
    """
    __slots__ = ("bid", "bid_qty", "ask", "ask_qty", "ts")
    def __init__(self, bid: float, bid_qty: Optional[float], ask: float, ask_qty: Optional[float], ts: float):
        self.bid, self.bid_qty, self.ask, self.ask_qty, self.ts = bid, bid_qty, ask, ask_qty, ts

def bps(x: float) -> float:
    return x * 10000.0


# ---------- CCXT Exchange ----------
class ExchangeClient:
    """
    Async ccxt wrapper for standard venues with an OKX public-retry.
    Special-cases CoinSpot to try fetch_ticker for bid/ask, then OB for sizes.
    Always returns dict: {"bid","ask","bid_qty","ask_qty","ts"}
    """
    def __init__(self, ex_id: str, credentials: Optional[dict] = None):
        self.id = ex_id
        self._klass = getattr(ccxt_async, ex_id)
        self._creds = credentials or {}
        self.ex = None  # create lazily
        self.markets_loaded = False
        self.symbol_map = set()
        self.needs_auth = False

    def _new_instance(self, creds: Dict):
        opts = {"enableRateLimit": True}
        if self.id == "okx":
            opts["options"] = {"defaultType": "spot"}
        if creds:
            for k, v in creds.items():
                if v:
                    opts[k] = v
        return self._klass(opts)

    async def _load_once(self) -> None:
        await self.ex.load_markets()
        self.markets_loaded = True
        self.symbol_map = set(self.ex.markets.keys())
        logger.info(f"[{self.id}] markets loaded: {len(self.symbol_map)}")

    async def load(self):
        self.ex = self._new_instance(self._creds)
        try:
            await self._load_once()
            return
        except Exception as e:
            msg = str(e)
            if self.id == "okx" and self._creds:
                logger.warning(f"[okx] load_markets failed with creds; retrying public-only. Error: {msg}")
                try:
                    await self.ex.close()
                except Exception:
                    pass
                self.ex = self._new_instance({})
                try:
                    await self._load_once()
                    return
                except Exception as e2:
                    try:
                        await self.ex.close()
                    except Exception:
                        pass
                    logger.error(f"[okx] public-only load_markets failed: {e2}")
                    self.ex = None
                    return
            else:
                try:
                    await self.ex.close()
                except Exception:
                    pass
                logger.error(f"[{self.id}] load_markets error: {e}")
                self.ex = None

    async def fetch_tob(self, symbol: str, depth: int = 5):
        """
        Return dict: {"bid": float, "ask": float, "bid_qty": Optional[float], "ask_qty": Optional[float], "ts": float}
        """
        if self.needs_auth or not self.ex:
            return None
        if symbol not in self.symbol_map:
            return None
        try:
            bid = ask = None
            bid_qty = ask_qty = None

            # ---- CoinSpot: prefer ticker for price; try OB for sizes if possible
            if self.id == "coinspot":
                try:
                    tk = await self.ex.fetch_ticker(symbol)
                    bid = tk.get("bid"); ask = tk.get("ask")
                except Exception:
                    pass
                # Try OB for sizes (and fallback prices if ticker empty)
                try:
                    ob = await self.ex.fetch_order_book(symbol, limit=depth)
                    bids = ob.get("bids", []); asks = ob.get("asks", [])
                    if bids:
                        if bid is None: bid = float(bids[0][0])
                        bid_qty = float(bids[0][1])
                    if asks:
                        if ask is None: ask = float(asks[0][0])
                        ask_qty = float(asks[0][1])
                except Exception as e:
                    msg = str(e).lower()
                    if "api key" in msg or 'requires "apikey"' in msg:
                        self.needs_auth = True
                        logger.warning("[coinspot] auth likely required for order book; add API key/secret (sizes may be None)")
                if bid is None or ask is None:
                    return None
                return {"bid": float(bid), "ask": float(ask), "bid_qty": bid_qty, "ask_qty": ask_qty, "ts": time.time()}

            # ---- all other venues: use OB
            ob = await self.ex.fetch_order_book(symbol, limit=depth)
            bids = ob.get("bids", []); asks = ob.get("asks", [])
            if not bids or not asks:
                return None
            bid = float(bids[0][0]); ask = float(asks[0][0])
            bid_qty = float(bids[0][1]) if len(bids[0]) > 1 and bids[0][1] is not None else None
            ask_qty = float(asks[0][1]) if len(asks[0]) > 1 and asks[0][1] is not None else None
            return {"bid": bid, "ask": ask, "bid_qty": bid_qty, "ask_qty": ask_qty, "ts": time.time()}

        except Exception as e:
            msg = str(e).lower()
            if 'requires "apikey"' in msg or "requires api key" in msg or "api key" in msg:
                self.needs_auth = True
                logger.warning(f"[{self.id}] auth required for order book; add API key/secret in .env to enable (sizes may be None)")
                return None
            logger.debug(f"[{self.id}] {symbol} fetch_order_book err: {e}")
            return None

    async def close(self):
        if self.ex:
            try:
                await self.ex.close()
            except Exception:
                pass


# ---------- Detector ----------
class ArbDetector:
    def __init__(
        self,
        symbols: List[str],
        exchange_ids: List[str],
        fees_bps: Dict[str, int],
        min_profit_bps: int,
        slippage_bps: int,
        poll_ms: int,
        out_dir: Path,
        log_dir: Path,
        creds_by_ex: Dict[str, dict],
        swyftx_opts: Dict[str, str],
        max_trade_aud: float = 250.0,         # current cap (user param)
    ):
        self.symbols = symbols
        self.exchange_ids = exchange_ids
        self.fees_bps = fees_bps
        self.min_profit_bps = min_profit_bps
        self.slippage_bps = slippage_bps
        self.poll_ms = poll_ms
        self.out_dir = out_dir
        self.log_dir = log_dir
        self.creds_by_ex = creds_by_ex
        self.swyftx_opts = swyftx_opts
        self.max_trade_aud = float(max_trade_aud)

        self.clients: Dict[str, object] = {}
        self.state: Dict[str, Dict[str, OrderBookTOB]] = {s: {} for s in symbols}
        self.snap_csv = out_dir / "tob_snapshots.csv"
        self.opps_csv = out_dir / "opps_live.csv"

        # diagnostics
        self._first_tick_logged: Set[Tuple[str, str]] = set()  # (ex_id, symbol)
        self.enabled_symbols: Dict[str, List[str]] = {}
        self._running_balance = 0.0  # cumulative expected PnL using max_trade_aud

    async def setup(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        logger.add(self.log_dir / "bot.log", rotation="10 MB", retention=10, level="DEBUG")

        # main snapshots csv
        if not self.snap_csv.exists():
            with self.snap_csv.open("w", newline="") as f:
                csv.writer(f).writerow([
                    "ts_iso","timestamp","symbol",
                    "buy_ex","buy_ask","sell_ex","sell_bid",
                    "gross_spread_bps","fees_bps","slippage_bps","net_spread_bps",
                    "meets_threshold"
                ])

        # opportunities csv (qualifying only)
        if not self.opps_csv.exists():
            with self.opps_csv.open("w", newline="") as f:
                csv.writer(f).writerow([
                    "ts_iso","symbol",
                    "buy_ex","buy_ask","buy_ask_qty",
                    "sell_ex","sell_bid","sell_bid_qty",
                    "net_spread_bps","max_trade_size_aud",
                    "qualifies_0p70","expected_profit_aud_at_cap","running_balance_aud"
                ])

        # Build & probe each exchange; failures don't crash the program.
        for ex in self.exchange_ids:
            try:
                if ex == "swyftx":
                    token = self.swyftx_opts.get("token", "")
                    api_key = self.swyftx_opts.get("api_key", "")
                    demo = self.swyftx_opts.get("demo") == "1"
                    refresh_url = self.swyftx_opts.get("refresh_url", "")
                    if not api_key and not token:
                        logger.warning("[swyftx] missing SWYFTX_API_KEY and SWYFTX_ACCESS_TOKEN; skipping")
                        continue
                    swx = SwyftxExchangeClient(
                        self.symbols,
                        access_token=token,
                        api_key=api_key,
                        demo=demo,
                        refresh_url=refresh_url,
                    )
                    await swx.load()
                    if swx.markets_loaded:
                        self.clients["swyftx"] = swx
                    else:
                        logger.warning(
                            f"[swyftx] token check failed; "
                            f"status={swx._last_status}, body_snippet={swx._last_body}, "
                            f"refresh_status={swx._last_refresh_status}, refresh_body={swx._last_refresh_body}"
                        )
                        await swx.close()
                    continue

                if ex == "okx":
                    # Prefer WS adapter; fallback to CCXT if WS yields 0 symbols.
                    use_ws = (os.getenv("OKX_WS_ENABLED", "1") == "1")
                    if use_ws:
                        channel = os.getenv("OKX_WS_ORDERBOOK_CHANNEL", "books5")
                        okx_ws = OkxWSExchangeClient(
                            symbols=self.symbols,
                            channel=channel,
                            api_key=self.creds_by_ex.get("okx", {}).get("apiKey") or "",
                            api_secret=self.creds_by_ex.get("okx", {}).get("secret") or "",
                            passphrase=self.creds_by_ex.get("okx", {}).get("password") or "",
                            public_ws_url=os.getenv("OKX_WS_PUBLIC_URL", OKX_PUBLIC_WS),
                            private_ws_url=os.getenv("OKX_WS_PRIVATE_URL", ""),
                        )
                        await okx_ws.load()
                        if okx_ws.markets_loaded:
                            self.clients["okx"] = okx_ws
                            continue
                        else:
                            await okx_ws.close()
                    # fallback: ccxt
                    cc = ExchangeClient("okx", credentials=self.creds_by_ex.get("okx"))
                    await cc.load()
                    if cc.markets_loaded:
                        self.clients["okx"] = cc
                    else:
                        await cc.close()
                        logger.warning("Removing okx: failed to load markets or auth not satisfied")
                    continue

                # CCXT for other exchanges
                cc = ExchangeClient(ex, credentials=self.creds_by_ex.get(ex))
                await cc.load()
                if cc.markets_loaded:
                    self.clients[ex] = cc
                else:
                    await cc.close()
                    logger.warning(f"Removing {ex}: failed to load markets or auth not satisfied")
            except Exception as e:
                logger.error(f"[{ex}] setup error: {e}")

        if not self.clients:
            raise RuntimeError("No exchanges available after setup()")

        # ---- Auto-filter symbols per exchange (poll only what the venue actually lists)
        self._compute_enabled_symbols()
        self._emit_enabled_symbols_file()

    def _compute_enabled_symbols(self):
        enabled: Dict[str, List[str]] = {}
        for ex_id, client in self.clients.items():
            listed = getattr(client, "symbol_map", set()) or set()
            want = set(self.symbols)
            have = sorted(list(want & set(listed)))
            if not have:
                logger.warning(f"[{ex_id}] No requested symbols listed on this venue; will skip polling.")
            else:
                logger.info(f"[{ex_id}] enabled symbols: {have}")
            enabled[ex_id] = have
        self.enabled_symbols = enabled

    def _emit_enabled_symbols_file(self):
        out = self.out_dir / f"enabled_symbols_{int(time.time())}.csv"
        with out.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["exchange", "symbol"])
            for ex_id, syms in self.enabled_symbols.items():
                for s in syms:
                    w.writerow([ex_id, s])
        logger.info(f"[SETUP] wrote enabled symbol map to {out}")

    async def _poll_exchange_symbol(self, ex_id: str, symbol: str):
        client = self.clients[ex_id]
        interval = max(self.poll_ms, 100) / 1000.0
        while True:
            tob = None
            try:
                d = await client.fetch_tob(symbol)
                if d:
                    tob = OrderBookTOB(
                        d["bid"], d.get("bid_qty"),
                        d["ask"], d.get("ask_qty"),
                        d["ts"]
                    )
                    if (ex_id, symbol) not in self._first_tick_logged:
                        self._first_tick_logged.add((ex_id, symbol))
                        logger.info(f"[FIRST] {ex_id} {symbol} bid={tob.bid:.8f} ask={tob.ask:.8f} "
                                    f"(bid_qty={tob.bid_qty}, ask_qty={tob.ask_qty})")
            except Exception as e:
                logger.debug(f"[{ex_id}] {symbol} polling error: {e}")
                tob = None
            if tob:
                self.state[symbol][ex_id] = tob
            await asyncio.sleep(interval)

    async def _writer_loop(self):
        while True:
            now = time.time()
            wrote = 0
            for symbol in self.symbols:
                books = self.state.get(symbol, {})
                if len(books) < 2:
                    continue

                best_sell_ex, best_sell_bid, best_sell_qty = None, -math.inf, None
                best_buy_ex, best_buy_ask, best_buy_qty = None, math.inf, None
                for ex_id, tob in books.items():
                    if tob.bid > best_sell_bid:
                        best_sell_ex, best_sell_bid, best_sell_qty = ex_id, tob.bid, tob.bid_qty
                    if tob.ask < best_buy_ask:
                        best_buy_ex, best_buy_ask, best_buy_qty = ex_id, tob.ask, tob.ask_qty
                if not best_buy_ex or not best_sell_ex or best_buy_ex == best_sell_ex:
                    continue

                gross = (best_sell_bid - best_buy_ask) / max(best_buy_ask, 1e-12)
                gross_bps = bps(gross)

                fees_total = int(self.fees_bps.get(best_buy_ex, 30)) + int(self.fees_bps.get(best_sell_ex, 30))
                net_bps = gross_bps - fees_total - self.slippage_bps
                meets = int(net_bps >= self.min_profit_bps)

                # --- write main snapshots row
                with self.snap_csv.open("a", newline="") as f:
                    csv.writer(f).writerow([
                        pd.Timestamp.utcnow().isoformat(),
                        f"{now:.3f}",
                        symbol,
                        best_buy_ex, f"{best_buy_ask:.8f}",
                        best_sell_ex, f"{best_sell_bid:.8f}",
                        f"{gross_bps:.2f}",
                        fees_total,
                        self.slippage_bps,
                        f"{net_bps:.2f}",
                        meets
                    ])
                wrote += 1

                # --- if it qualifies, also write opps_live row (with sizes & PnL)
                if meets:
                    # compute TOB-constrained max AUD size if both sizes are present
                    tob_max_aud = None
                    if (best_buy_qty is not None) and (best_sell_qty is not None):
                        try:
                            tob_max_aud = min(best_buy_qty * best_buy_ask, best_sell_qty * best_sell_bid)
                        except Exception:
                            tob_max_aud = None

                    # expected profit at configured cap (e.g., 250 AUD)
                    expected_pnl_cap = self.max_trade_aud * (net_bps / 10000.0)
                    self._running_balance += expected_pnl_cap

                    with self.opps_csv.open("a", newline="") as f:
                        csv.writer(f).writerow([
                            pd.Timestamp.utcnow().isoformat(),
                            symbol,
                            best_buy_ex, f"{best_buy_ask:.8f}", (f"{best_buy_qty:.8f}" if best_buy_qty is not None else ""),
                            best_sell_ex, f"{best_sell_bid:.8f}", (f"{best_sell_qty:.8f}" if best_sell_qty is not None else ""),
                            f"{net_bps:.2f}",
                            (f"{tob_max_aud:.2f}" if tob_max_aud is not None else f"{self.max_trade_aud:.2f}"),
                            "YES",
                            f"{expected_pnl_cap:.2f}",
                            f"{self._running_balance:.2f}",
                        ])

                    logger.info(
                        f"[OPP] {symbol} buy {best_buy_ex}@{best_buy_ask:.4f} "
                        f"(qty {best_buy_qty}) -> sell {best_sell_ex}@{best_sell_bid:.4f} "
                        f"(qty {best_sell_qty}) | net {net_bps:.1f} bps | "
                        f"exp_pnl_at_{self.max_trade_aud:.0f}AUD={expected_pnl_cap:.2f} "
                        f"(run={self._running_balance:.2f})"
                    )

            if wrote == 0:
                # emit a soft heartbeat to help diagnose why CSV is empty
                diag = {s: len(self.state.get(s, {})) for s in self.symbols}
                have_any = {s: n for s, n in diag.items() if n > 0}
                if have_any:
                    logger.info(f"[HEALTH] venues per symbol (>=1 only): {have_any}")
                else:
                    logger.info("[HEALTH] no venues delivering TOB yet; waiting for first ticks...")
            await asyncio.sleep(1.0)

    async def _health_loop(self):
        # periodic snapshot of which exchanges are updating
        while True:
            ex_counts: Dict[str, int] = {}
            for s, m in self.state.items():
                for ex_id in m.keys():
                    ex_counts[ex_id] = ex_counts.get(ex_id, 0) + 1
            if ex_counts:
                logger.info(f"[HEALTH] per-exchange symbol coverage: {ex_counts}")
            await asyncio.sleep(15)

    async def run(self):
        tasks = []
        for ex_id, client in self.clients.items():
            ex_symbols = self.enabled_symbols.get(ex_id, [])
            if not ex_symbols:
                logger.warning(f"[{ex_id}] No enabled symbols after auto-filter; skipping")
                continue
            for s in ex_symbols:
                tasks.append(asyncio.create_task(self._poll_exchange_symbol(ex_id, s)))
            logger.info(f"[{ex_id}] polling {len(ex_symbols)} symbols")

        if not tasks:
            raise RuntimeError("No symbols to poll across all exchanges")

        writer = asyncio.create_task(self._writer_loop())
        health = asyncio.create_task(self._health_loop())

        stop = asyncio.Future()
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_running_loop().add_signal_handler(sig, lambda s=sig: stop.set_result(True))
        await stop

        [t.cancel() for t in tasks]; writer.cancel(); health.cancel()

    async def close(self):
        await asyncio.gather(*[
            c.close() for c in self.clients.values() if hasattr(c, "close")
        ], return_exceptions=True)


# ---------- Entrypoint ----------
def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

async def main():
    cfg = load_config("config.yaml")
    load_dotenv()

    out_dir = Path(cfg.get("out_dir", "./out"))
    log_dir = Path(cfg.get("log_dir", "./logs"))

    creds = {
        "coinspot": {"apiKey": os.getenv("COINSPOT_API_KEY"), "secret": os.getenv("COINSPOT_SECRET")},
        "kraken": {"apiKey": os.getenv("KRAKEN_API_KEY"), "secret": os.getenv("KRAKEN_SECRET")},
        "independentreserve": {"apiKey": os.getenv("INDEPENDENTRESERVE_API_KEY"), "secret": os.getenv("INDEPENDENTRESERVE_SECRET")},
        "btcmarkets": {"apiKey": os.getenv("BTCMARKETS_API_KEY"), "secret": os.getenv("BTCMARKETS_SECRET")},
        "okx": {"apiKey": os.getenv("OKX_API_KEY"), "secret": os.getenv("OKX_SECRET"), "password": os.getenv("OKX_PASSPHRASE")},
    }

    swyftx_opts = {
        "token": os.getenv("SWYFTX_ACCESS_TOKEN", ""),
        "api_key": os.getenv("SWYFTX_API_KEY", ""),
        "demo": "1" if os.getenv("SWYFTX_DEMO", "0") == "1" else "0",
        "refresh_url": os.getenv("SWYFTX_REFRESH_URL", "").strip(),
    }

    det = ArbDetector(
        symbols=cfg["symbols"],
        exchange_ids=cfg["exchanges"],
        fees_bps=cfg.get("fees_bps", {}),
        min_profit_bps=int(cfg.get("min_profit_bps", 70)),
        slippage_bps=int(cfg.get("slippage_bps", 10)),
        poll_ms=int(cfg.get("poll_ms", 500)),
        out_dir=out_dir,
        log_dir=log_dir,
        creds_by_ex=creds,
        swyftx_opts=swyftx_opts,
        max_trade_aud=float(cfg.get("max_trade_aud", 250.0)),  # NEW: uses your current cap
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
