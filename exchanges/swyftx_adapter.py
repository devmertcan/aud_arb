from __future__ import annotations
import aiohttp
import time
from typing import Optional, Tuple, Dict, Iterable

DEFAULT_BASE = "https://api.swyftx.com.au"
DEMO_BASE = "https://api.demo.swyftx.com.au"

class SwyftxAsyncClient:
    """
    Minimal async client for Swyftx effective bid/ask.
    Auth: Bearer <ACCESS_TOKEN>
    Tries multiple endpoint shapes to be resilient.
    """
    def __init__(self, access_token: str, base_url: str = DEFAULT_BASE, timeout: float = 4.5):
        self.base_url = base_url.rstrip("/")
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None
        self._access_token = access_token
        self._working_shape: Optional[int] = None  # 0,1,2 as below
        self.last_ping_status: Optional[int] = None
        self.last_ping_body_snippet: Optional[str] = None

    async def _session_get(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                headers={
                    "Authorization": f"Bearer {self._access_token}",
                    "Accept": "application/json",
                    "User-Agent": "aud-arb/1.0",
                },
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def ping_user(self) -> bool:
        """Token sanity check with status/body snippet for diagnostics."""
        try:
            s = await self._session_get()
            async with s.get(f"{self.base_url}/user") as r:
                self.last_ping_status = r.status
                try:
                    txt = await r.text()
                except Exception:
                    txt = ""
                self.last_ping_body_snippet = (txt or "")[:160]
                return r.status == 200
        except Exception:
            self.last_ping_status = None
            self.last_ping_body_snippet = None
            return False

    async def get_bid_ask(self, base: str, quote: str) -> Optional[Tuple[float, float, float]]:
        shapes = (
            ("/markets/price", {"primaryCurrencyCode": base, "secondaryCurrencyCode": quote}),
            ("/markets/price", {"primary_currency_code": base, "secondary_currency_code": quote}),
            (f"/markets/price/{base}/{quote}", None),
        )
        s = await self._session_get()

        order = (
            [self._working_shape] + [i for i in range(len(shapes)) if i != self._working_shape]
            if self._working_shape is not None else range(len(shapes))
        )

        for idx in order:
            path, params = shapes[idx]
            url = f"{self.base_url}{path}"
            try:
                async with s.get(url, params=params) as r:
                    if r.status != 200:
                        continue
                    data = await r.json()
                    bid, ask = self._extract_bid_ask(data)
                    if bid is not None and ask is not None:
                        self._working_shape = idx
                        return bid, ask, time.time()
            except Exception:
                continue
        return None

    @staticmethod
    def _extract_bid_ask(payload: Dict) -> Tuple[Optional[float], Optional[float]]:
        """Normalize payload into (bid, ask)."""
        if not isinstance(payload, dict):
            return (None, None)

        if "ask" in payload and "bid" in payload:
            return float(payload["bid"]), float(payload["ask"])
        if "buy" in payload and "sell" in payload:
            return float(payload["sell"]), float(payload["buy"])

        for key in ("price", "data", "result"):
            obj = payload.get(key)
            if isinstance(obj, dict):
                if "ask" in obj and "bid" in obj:
                    return float(obj["bid"]), float(obj["ask"])
                if "buy" in obj and "sell" in obj:
                    return float(obj["sell"]), float(obj["buy"])

        for k in ("lastPrice", "last", "price"):
            if k in payload and isinstance(payload[k], (int, float)):
                p = float(payload[k]); return p, p

        return (None, None)


class SwyftxExchangeClient:
    """
    Looks like our CCXT ExchangeClient:
      - load(): sets markets_loaded (via token check)
      - fetch_tob(): returns dict {'bid','ask','ts'}
      - close(): closes aiohttp
      - symbol_map: configured symbols
    """
    def __init__(self, symbols: Iterable[str], access_token: str, demo: bool = False):
        self.id = "swyftx"
        base = DEMO_BASE if demo else DEFAULT_BASE
        self.client = SwyftxAsyncClient(access_token=access_token, base_url=base)
        self.symbol_map = set(symbols)
        self.markets_loaded = False
        self.needs_auth = False
        # For debug visibility (read by caller after load())
        self._last_status = None
        self._last_body = None

    async def load(self):
        ok = await self.client.ping_user()
        self._last_status = self.client.last_ping_status
        self._last_body = self.client.last_ping_body_snippet
        if not ok:
            self.needs_auth = True
            self.markets_loaded = False
            return
        self.markets_loaded = True

    async def fetch_tob(self, symbol: str):
        if self.needs_auth or symbol not in self.symbol_map:
            return None
        try:
            base, quote = symbol.split("/")
            res = await self.client.get_bid_ask(base, quote)
            if not res:
                return None
            bid, ask, ts = res
            return {"bid": float(bid), "ask": float(ask), "ts": ts}
        except Exception:
            return None

    async def close(self):
        await self.client.close()
