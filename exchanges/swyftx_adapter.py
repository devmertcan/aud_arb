# /opt/aud_arb/exchanges/swyftx_adapter.py
from __future__ import annotations
import os, time, json, asyncio
from typing import Dict, List, Optional
import aiohttp
from loguru import logger

SWYFTX_API_BASE = "https://api.swyftx.com.au"   # prod
SWYFTX_API_DEMO = "https://api.demo.swyftx.com.au"

class SwyftxExchangeClient:
    """
    Minimal Swyftx reader for TOB price; uses access token (Bearer) or refresh flow.
    Returns dict: {"bid","ask","bid_qty","ask_qty","ts"} — qty may be None.
    """
    def __init__(self, symbols: List[str], access_token: str, api_key: str, demo: bool = False, refresh_url: str = ""):
        self.id = "swyftx"
        self.symbols_req = symbols
        self.symbol_map = set(symbols)  # assume all; API discovery can be added
        self.markets_loaded = True
        self.needs_auth = False

        self._access_token = access_token or ""
        self._api_key = api_key or ""
        self._refresh_url = refresh_url.strip()
        self._base = SWYFTX_API_DEMO if demo else SWYFTX_API_BASE
        self._session: Optional[aiohttp.ClientSession] = None

        # diagnostics
        self._last_status = None
        self._last_body = None
        self._last_refresh_status = None
        self._last_refresh_body = None

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
                headers={"User-Agent": "aud-arb/swyftx/1.2", "Accept": "application/json"},
            )

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _auth_headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json", "User-Agent": "aud-arb/swyftx/1.2"}
        if self._access_token:
            h["Authorization"] = f"Bearer {self._access_token}"
        return h

    async def _get(self, path: str):
        await self._ensure_session()
        url = self._base + path
        try:
            async with self._session.get(url, headers=await self._auth_headers()) as r:
                self._last_status = r.status
                txt = await r.text()
                self._last_body = txt[:200]
                if r.status == 401 and self._refresh_url:
                    ok = await self.refresh_access_token()
                    if ok:
                        return await self._get(path)
                if r.status != 200:
                    return None
                try:
                    return json.loads(txt)
                except Exception:
                    return None
        except Exception:
            return None

    async def refresh_access_token(self) -> bool:
        """Refresh via user-provided endpoint that returns {"access_token": "..."}"""
        if not self._refresh_url:
            return False
        await self._ensure_session()
        try:
            async with self._session.get(self._refresh_url, headers={"X-Api-Key": self._api_key}) as r:
                self._last_refresh_status = r.status
                body = await r.text()
                self._last_refresh_body = body[:200]
                if r.status != 200:
                    return False
                js = json.loads(body)
                token = js.get("access_token") or js.get("token")
                if not token:
                    return False
                self._access_token = token
                logger.info("[swyftx] access token refreshed")
                return True
        except Exception:
            return False

    async def load(self):
        # sanity check: call a lightweight endpoint, tolerate failures
        _ = await self._get("/markets/info")  # not strictly required
        self.markets_loaded = True

    async def fetch_tob(self, symbol: str):
        """
        Try an orderbook endpoint if accessible; else use ticker.
        This returns dict with keys present; sizes may be None.
        """
        # Normalize symbol to Swyftx format if needed (e.g., BTC/AUD -> BTCAUD); keep simple:
        base, quote = symbol.split("/")
        market = f"{base}{quote}"

        # Try orderbook first (to get sizes)
        ob = await self._get(f"/markets/{market}/orderbook")
        if ob and isinstance(ob, dict):
            try:
                bids = ob.get("bids") or []
                asks = ob.get("asks") or []
                if bids and asks:
                    bid = float(bids[0]["price"]); bid_qty = float(bids[0].get("amount") or bids[0].get("quantity"))
                    ask = float(asks[0]["price"]); ask_qty = float(asks[0].get("amount") or asks[0].get("quantity"))
                    return {"bid": bid, "ask": ask, "bid_qty": bid_qty, "ask_qty": ask_qty, "ts": time.time()}
            except Exception:
                pass

        # Fallback: ticker (no sizes)
        tk = await self._get(f"/markets/{market}/ticker")
        if tk and isinstance(tk, dict):
            try:
                bid = float(tk.get("bid"))
                ask = float(tk.get("ask"))
                if bid and ask:
                    return {"bid": bid, "ask": ask, "bid_qty": None, "ask_qty": None, "ts": time.time()}
            except Exception:
                pass

        return None
