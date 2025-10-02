#!/usr/bin/env python3
from __future__ import annotations

import asyncio, os, csv, time, math, signal, itertools
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

def now_iso() -> str:
    return pd.Timestamp.utcnow().isoformat()


# ---------- CCXT Exchange ----------
class ExchangeClient:
    """
    Async ccxt wrapper for standard venues with an OKX public-retry.
    Returns dict: {"bid","ask","bid_qty","ask_qty","ts"}
    """
    def __init__(self, ex_id: str, credentials: Optional[dict] = None):
        self.id = ex_id
        self._klass = getattr(ccxt_async, ex_id)
        self._creds = credentials or {}
        self.ex = None
        self.markets_loaded = False
        self.symbol_map: Set[str] = set()
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
                logger.warning(f"[{self.id}] auth required for order book; add API key/secret in .env")
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
        max_trade_aud: float = 250.0,
    ):
        self.base_symbols = symbols                      # requested (mostly */AUD)
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
        self.state: Dict[str, Dict[str, OrderBookTOB]] = {}  # symbol -> {ex_id: tob}
        self.snap_csv = out_dir / "tob_snapshots.csv"
        self.opps_csv = out_dir / "opps_live.csv"
        self.opps_all_csv = out_dir / "opps_live_all.csv"
        self.tri_csv = out_dir / "tri_arb_live.csv"
        self.tri_all_csv = out_dir / "tri_arb_all.csv"

        # diagnostics
        self._first_tick_logged: Set[Tuple[str, str]] = set()
        self.enabled_symbols: Dict[str, List[str]] = {}
        self._running_balance = 0.0  # cumulative expected PnL using max_trade_aud

        # per-exchange computed asset lists (coins with AUD pairs)
        self.assets_per_ex: Dict[str, Set[str]] = {}

    # ---------- setup ----------
    async def setup(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        logger.add(self.log_dir / "bot.log", rotation="10 MB", retention=10, level="DEBUG")

        # CSV headers
        if not self.snap_csv.exists():
            with self.snap_csv.open("w", newline="") as f:
                csv.writer(f).writerow([
                    "ts_iso","timestamp","symbol",
                    "buy_ex","buy_ask","sell_ex","sell_bid",
                    "gross_spread_bps","fees_bps","slippage_bps","net_spread_bps",
                    "meets_threshold"
                ])
        if not self.opps_csv.exists():
            with self.opps_csv.open("w", newline="") as f:
                csv.writer(f).writerow([
                    "ts_iso","symbol",
                    "buy_ex","buy_ask","buy_ask_qty",
                    "sell_ex","sell_bid","sell_bid_qty",
                    "net_spread_bps","max_trade_size_aud",
                    "qualifies","expected_profit_aud_at_cap","running_balance_aud"
                ])
        if not self.opps_all_csv.exists():
            with self.opps_all_csv.open("w", newline="") as f:
                csv.writer(f).writerow([
                    "ts_iso","symbol",
                    "buy_ex","buy_ask","buy_ask_qty",
                    "sell_ex","sell_bid","sell_bid_qty",
                    "gross_spread_bps","fees_bps","slippage_bps",
                    "net_spread_bps","max_trade_size_aud",
                    "qualifies","expected_profit_aud_at_cap"
                ])
        if not self.tri_csv.exists():
            with self.tri_csv.open("w", newline="") as f:
                csv.writer(f).writerow([
                    "ts_iso","exchange","path",
                    "sym1","side1","px1",
                    "sym2","side2","px2",
                    "sym3","side3","px3",
                    "net_spread_bps","expected_profit_aud_at_cap","qualifies"
                ])
        if not self.tri_all_csv.exists():
            with self.tri_all_csv.open("w", newline="") as f:
                csv.writer(f).writerow([
                    "ts_iso","exchange","path",
                    "sym1","side1","px1",
                    "sym2","side2","px2",
                    "sym3","side3","px3",
                    "net_spread_bps","expected_profit_aud_at_cap","qualifies"
                ])

        # -------- Phase 1: load market maps (CCXT) to compute symbols for triangles
        symbol_maps: Dict[str, Set[str]] = {}
        temp_okx_map: Optional[Set[str]] = None

        for ex in self.exchange_ids:
            if ex == "swyftx":
                # assume only AUD pairs as requested; triangle may be limited
                symbol_maps[ex] = set(self.base_symbols)
                continue
            if ex == "okx":
                # load markets via CCXT public (temp) to discover cross symbols
                tmp = ExchangeClient("okx", credentials=self.creds_by_ex.get("okx"))
                await tmp.load()
                temp_okx_map = tmp.symbol_map.copy() if tmp.markets_loaded else set()
                symbol_maps[ex] = temp_okx_map
                await tmp.close()
                continue

            # regular ccxt client (kept)
            cc = ExchangeClient(ex, credentials=self.creds_by_ex.get(ex))
            await cc.load()
            if cc.markets_loaded:
                self.clients[ex] = cc
                symbol_maps[ex] = cc.symbol_map.copy()
            else:
                await cc.close()
                logger.warning(f"Removing {ex}: failed to load markets or auth not satisfied")
                symbol_maps[ex] = set()

        # -------- Compute per-exchange symbols: AUD pairs + needed cross pairs for triangles
        self.enabled_symbols, self.assets_per_ex = self._compute_symbols_for_polling(symbol_maps)
        self._emit_enabled_symbols_file()

        # -------- Phase 2: build final clients needing symbol lists (OKX WS + Swyftx)
        # OKX: WS client with expanded set (AUD + cross pairs)
        if "okx" in self.exchange_ids:
            okx_symbols = self.enabled_symbols.get("okx", [])
            okx_ws = OkxWSExchangeClient(
                symbols=okx_symbols,
                channel=os.getenv("OKX_WS_ORDERBOOK_CHANNEL", "books5"),
                api_key=self.creds_by_ex.get("okx", {}).get("apiKey") or "",
                api_secret=self.creds_by_ex.get("okx", {}).get("secret") or "",
                passphrase=self.creds_by_ex.get("okx", {}).get("password") or "",
                public_ws_url=os.getenv("OKX_WS_PUBLIC_URL", OKX_PUBLIC_WS),
            )
            await okx_ws.load()
            if okx_ws.markets_loaded:
                self.clients["okx"] = okx_ws
            else:
                await okx_ws.close()
                logger.warning("Removing okx: failed to start WS")
        # Swyftx
        if "swyftx" in self.exchange_ids:
            token = self.swyftx_opts.get("token", "")
            api_key = self.swyftx_opts.get("api_key", "")
            demo = self.swyftx_opts.get("demo") == "1"
            refresh_url = self.swyftx_opts.get("refresh_url", "")
            if api_key or token:
                swx = SwyftxExchangeClient(self.base_symbols, token, api_key, demo, refresh_url)
                await swx.load()
                if swx.markets_loaded:
                    self.clients["swyftx"] = swx
                else:
                    await swx.close()
                    logger.warning("[swyftx] failed to load; skipping")
            else:
                logger.warning("[swyftx] missing SWYFTX_API_KEY or token; skipping")

        # init state dict for all symbols
        all_symbols = set()
        for lst in self.enabled_symbols.values():
            all_symbols.update(lst)
        self.state = {s: {} for s in sorted(all_symbols)}

        if not self.clients:
            raise RuntimeError("No exchanges available after setup()")

    def _compute_symbols_for_polling(self, symbol_maps: Dict[str, Set[str]]):
        """
        For each exchange, start with the requested AUD pairs, then add needed cross pairs
        (e.g., ETH/BTC or BTC/ETH) so we can do AUD→B→C→AUD triangles.
        """
        enabled: Dict[str, List[str]] = {}
        assets_per_ex: Dict[str, Set[str]] = {}

        # Extract requested AUD assets like ["BTC","ETH",...]
        req_assets = set()
        for s in self.base_symbols:
            try:
                base, quote = s.split("/")
                if quote == "AUD":
                    req_assets.add(base)
            except ValueError:
                continue

        for ex_id, market_syms in symbol_maps.items():
            if not market_syms:
                enabled[ex_id] = []
                assets_per_ex[ex_id] = set()
                continue

            # 1) Aud pairs present on this venue
            aud_syms = [s for s in self.base_symbols if s in market_syms]
            assets_here = set()
            for s in aud_syms:
                base, _ = s.split("/")
                assets_here.add(base)

            # 2) For each pair of assets with AUD legs, add cross symbol if available
            extra_cross = set()
            for a, b in itertools.combinations(sorted(assets_here), 2):
                s1 = f"{a}/{b}"
                s2 = f"{b}/{a}"
                if s1 in market_syms:
                    extra_cross.add(s1)
                if s2 in market_syms:
                    extra_cross.add(s2)

            poll_list = sorted(set(aud_syms) | extra_cross)
            enabled[ex_id] = poll_list
            assets_per_ex[ex_id] = assets_here

            if poll_list:
                logger.info(f"[{ex_id}] enabled symbols: {poll_list}")
            else:
                logger.warning(f"[{ex_id}] no enabled symbols")

        return enabled, assets_per_ex

    def _emit_enabled_symbols_file(self):
        out = self.out_dir / f"enabled_symbols_{int(time.time())}.csv"
        with out.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["exchange", "symbol"])
            for ex_id, syms in self.enabled_symbols.items():
                for s in syms:
                    w.writerow([ex_id, s])
        logger.info(f"[SETUP] wrote enabled symbol map to {out}")

    # ---------- polling ----------
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
                        logger.info(
                            f"[FIRST] {ex_id} {symbol} bid={tob.bid:.8f} ask={tob.ask:.8f} "
                            f"(bid_qty={tob.bid_qty}, ask_qty={tob.ask_qty})"
                        )
            except Exception as e:
                logger.debug(f"[{ex_id}] {symbol} polling error: {e}")
                tob = None
            if tob:
                self.state[symbol][ex_id] = tob
            await asyncio.sleep(interval)

    # ---------- cross-exchange writer ----------
    async def _writer_loop_cross(self):
        while True:
            now = time.time()
            wrote = 0
            for symbol in [s for s in self.state.keys() if s.endswith("/AUD")]:
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

                # main snapshots row
                with self.snap_csv.open("a", newline="") as f:
                    csv.writer(f).writerow([
                        now_iso(),
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

                # TOB-constrained notional size (AUD), if both sizes known; else cap
                tob_max_aud = None
                if (best_buy_qty is not None) and (best_sell_qty is not None):
                    try:
                        tob_max_aud = min(best_buy_qty * best_buy_ask, best_sell_qty * best_sell_bid)
                    except Exception:
                        tob_max_aud = None
                max_aud_for_row = tob_max_aud if tob_max_aud is not None else self.max_trade_aud
                expected_pnl_cap = self.max_trade_aud * (net_bps / 10000.0)

                # always write ALL crosses file
                with self.opps_all_csv.open("a", newline="") as f:
                    csv.writer(f).writerow([
                        now_iso(), symbol,
                        best_buy_ex, f"{best_buy_ask:.8f}", (f"{best_buy_qty:.8f}" if best_buy_qty is not None else ""),
                        best_sell_ex, f"{best_sell_bid:.8f}", (f"{best_sell_qty:.8f}" if best_sell_qty is not None else ""),
                        f"{gross_bps:.2f}", fees_total, self.slippage_bps,
                        f"{net_bps:.2f}", f"{max_aud_for_row:.2f}",
                        "YES" if meets else "NO", f"{expected_pnl_cap:.2f}"
                    ])

                if meets:
                    self._running_balance += expected_pnl_cap
                    with self.opps_csv.open("a", newline="") as f:
                        csv.writer(f).writerow([
                            now_iso(), symbol,
                            best_buy_ex, f"{best_buy_ask:.8f}", (f"{best_buy_qty:.8f}" if best_buy_qty is not None else ""),
                            best_sell_ex, f"{best_sell_bid:.8f}", (f"{best_sell_qty:.8f}" if best_sell_qty is not None else ""),
                            f"{net_bps:.2f}", f"{max_aud_for_row:.2f}",
                            "YES", f"{expected_pnl_cap:.2f}", f"{self._running_balance:.2f}"
                        ])

            if wrote == 0:
                have_any = {s: len(self.state.get(s, {})) for s in self.state if len(self.state.get(s, {})) > 0}
                if have_any:
                    logger.info(f"[HEALTH] venues per symbol (>=1 only): {have_any}")
                else:
                    logger.info("[HEALTH] no venues delivering TOB yet; waiting for first ticks...")
            await asyncio.sleep(1.0)

    # ---------- triangular arbitrage (same exchange) ----------
    def _get_tob(self, ex_id: str, symbol: str) -> Optional[OrderBookTOB]:
        tob = self.state.get(symbol, {}).get(ex_id)
        return tob

    def _convert(self, ex_id: str, amt: float, from_ccy: str, to_ccy: str) -> Optional[Tuple[float,str,str,float]]:
        """
        Convert amt of from_ccy to to_ccy using available symbol on the same exchange.
        Returns (to_amount, symbol_used, side, price_used) or None.
          - If symbol is X/Y and from_ccy==X (sell X->Y): use bid(X/Y)  (side='sell')
          - If symbol is Y/X and from_ccy==X (buy Y with X): use ask(Y/X) (side='buy')
        Fees+slippage are applied multiplicatively on the output.
        """
        # Prefer direct: from/to
        sym1 = f"{from_ccy}/{to_ccy}"
        sym2 = f"{to_ccy}/{from_ccy}"

        fee = int(self.fees_bps.get(ex_id, 30)) / 10000.0
        slip = self.slippage_bps / 10000.0

        tob = self._get_tob(ex_id, sym1)
        if tob:  # sell base for quote at bid
            out = amt * float(tob.bid)
            out *= (1.0 - fee) * (1.0 - slip)
            return out, sym1, "sell", float(tob.bid)

        tob = self._get_tob(ex_id, sym2)
        if tob:  # buy base (to_ccy) with quote (from_ccy) at ask
            if tob.ask <= 0:
                return None
            out = amt / float(tob.ask)
            out *= (1.0 - fee) * (1.0 - slip)
            return out, sym2, "buy", float(tob.ask)

        return None

    async def _writer_loop_tri(self):
        """
        For each exchange, attempt AUD -> B -> C -> AUD cycles using available symbols.
        Only considers assets that have */AUD listed on that exchange (to keep it realistic).
        """
        while True:
            for ex_id, assets in self.assets_per_ex.items():
                if not assets:
                    continue
                # all 2-combinations among assets
                for b, c in itertools.combinations(sorted(assets), 2):
                    # need: B/AUD, C/AUD, and at least one of B/C or C/B (handled inside _convert)
                    if f"{b}/AUD" not in self.state or f"{c}/AUD" not in self.state:
                        continue
                    # base AUD notional for comparability
                    aud_in = self.max_trade_aud

                    # AUD -> B (buy B with AUD): need B/AUD ask
                    step1 = self._convert(ex_id, aud_in, "AUD", b)
                    if not step1:
                        continue
                    b_amt, sym1, side1, px1 = step1

                    # B -> C
                    step2 = self._convert(ex_id, b_amt, b, c)
                    if not step2:
                        continue
                    c_amt, sym2, side2, px2 = step2

                    # C -> AUD (sell C for AUD): need C/AUD bid
                    step3 = self._convert(ex_id, c_amt, c, "AUD")
                    if not step3:
                        continue
                    aud_out, sym3, side3, px3 = step3

                    gross_ret = (aud_out / max(aud_in, 1e-12)) - 1.0
                    net_bps = bps(gross_ret)  # fees+slip already inside steps
                    qualifies = net_bps >= self.min_profit_bps
                    exp_pnl_cap = self.max_trade_aud * (net_bps / 10000.0)

                    path = f"AUD->{b}->{c}->AUD"

                    # write ALL
                    with self.tri_all_csv.open("a", newline="") as f:
                        csv.writer(f).writerow([
                            now_iso(), ex_id, path,
                            sym1, side1, f"{px1:.8f}",
                            sym2, side2, f"{px2:.8f}",
                            sym3, side3, f"{px3:.8f}",
                            f"{net_bps:.2f}", f"{exp_pnl_cap:.2f}", "YES" if qualifies else "NO"
                        ])

                    if qualifies:
                        with self.tri_csv.open("a", newline="") as f:
                            csv.writer(f).writerow([
                                now_iso(), ex_id, path,
                                sym1, side1, f"{px1:.8f}",
                                sym2, side2, f"{px2:.8f}",
                                sym3, side3, f"{px3:.8f}",
                                f"{net_bps:.2f}", f"{exp_pnl_cap:.2f}", "YES"
                            ])
                        logger.info(f"[TRI] {ex_id} {path} net={net_bps:.2f} bps pnl@{self.max_trade_aud:.0f}={exp_pnl_cap:.2f}")

            await asyncio.sleep(1.0)

    async def _health_loop(self):
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
                logger.warning(f"[{ex_id}] No enabled symbols; skipping")
                continue
            for s in ex_symbols:
                tasks.append(asyncio.create_task(self._poll_exchange_symbol(ex_id, s)))
            logger.info(f"[{ex_id}] polling {len(ex_symbols)} symbols")

        if not tasks:
            raise RuntimeError("No symbols to poll across all exchanges")

        cross_writer = asyncio.create_task(self._writer_loop_cross())
        tri_writer = asyncio.create_task(self._writer_loop_tri())
        health = asyncio.create_task(self._health_loop())

        stop = asyncio.Future()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                asyncio.get_running_loop().add_signal_handler(sig, lambda s=sig: stop.set_result(True))
            except NotImplementedError:
                pass
        await stop

        [t.cancel() for t in tasks]; cross_writer.cancel(); tri_writer.cancel(); health.cancel()

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
        min_profit_bps=int(cfg.get("min_profit_bps", 50)),   # 0.50% detection
        slippage_bps=int(cfg.get("slippage_bps", 10)),
        poll_ms=int(cfg.get("poll_ms", 500)),
        out_dir=out_dir,
        log_dir=log_dir,
        creds_by_ex=creds,
        swyftx_opts=swyftx_opts,
        max_trade_aud=float(cfg.get("max_trade_aud", 250.0)),
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
