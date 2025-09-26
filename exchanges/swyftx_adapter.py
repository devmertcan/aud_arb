from __future__ import annotations
import aiohttp
import asyncio
import time
from typing import Optional, Tuple, Dict, Iterable

DEFAULT_BASE = "https://api.swyftx.com.au"
DEMO_BASE = "https://api.demo.swyftx.com.au"

class SwyftxAsyncClient:
    """
    Async client for Swyftx pricing with:
      - Bearer <access_token> auth for data calls
      - API key based refresh to mint/rotate access tokens
      - Auto refresh+retry on 401/403
    """
    def __init__(
        self,
        access_token: str = "",
        api_key: str = "",
        base_url: str = DEFAULT_BASE,
        timeout: float = 4.5,
        refresh_url_override: str = "",
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None

        self._access_token = access_token or ""
        self._api_key = api_key or ""
        self._refresh_url_override = refresh_url_override.strip()

        self._working_shape: Optional[int] = None  # price endpoint shape
        self._refresh_lock = asyncio.Lock()

        # ping diagnostics
        self.last_ping_status: Optional[int] = None
        self.last_ping_body_snippet: Optional[str] = None

        # refresh diagnostics
        self.last_refresh_status: Optional[int] = None
        self.last_refresh_body_snippet: Optional[str] = None

    # ---------- session helpers ----------
    def _auth_headers(self) -> Dict[str, str]:
        hdrs = {
            "Accept": "application/json",
            "User-Agent": "aud-arb/1.0",
        }
        if self._access_token:
            hdrs["Authorization"] = f"Bearer {self._access_token}"
        return hdrs

    async def _session_get(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout, headers=self._auth_headers())
        return self._session

    async def _reset_session_with_token(self, token: str):
        self._access_token = token
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = aiohttp.ClientSession(timeout=self.timeout, headers=self._auth_headers())

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ---------- token refresh ----------
    async def refresh_access_token(self) -> bool:
        """Mint/rotate a new access token using API key. Tries multiple endpoint shapes."""
        if not self._api_key:
            self.last_refresh_status = None
            self.last_refresh_body_snippet = "No API key configured"
            return False

        async with self._refresh_lock:
            # if another coroutine already refreshed while we waited, bail
            if self._access_token and self.last_ping_status == 200:
                return True

            s = await self._session_get()

            # explicit override (single attempt)
            if self._refresh_url_override:
                ok = await self._try_refresh_once(s, self._refresh_url_override)
                return ok

            # candidate endpoints and shapes (headers & bodies)
            bases = [self.base_url]
            paths = [
                "/auth/refresh-access-token",
                "/auth/refreshAccessToken",
                "/auth/refresh",
                "/auth/refresh_token",
                "/auth/token/refresh",
            ]

            async def attempts():
                # 1) Bearer <api_key>, POST no body
                for b in bases:
                    for p in paths:
                        yield f"{b}{p}", {"Authorization": f"Bearer {self._api_key}"}, None, None
                # 2) X-API-KEY header
                for b in bases:
                    for p in paths:
                        yield f"{b}{p}", {"X-API-KEY": self._api_key}, None, None
                # 3) JSON body {"apiKey": "..."}
                for b in bases:
                    for p in paths:
                        yield f"{b}{p}", {}, {"apiKey": self._api_key}, "json"
                # 4) form body apiKey=...
                for b in bases:
                    for p in paths:
                        yield f"{b}{p}", {}, {"apiKey": self._api_key}, "form"

            async for _ in _aiter(attempts()):
                pass  # linter

            for url, hdrs, body, kind in attempts():
                ok = await self._try_refresh_once(s, url, hdrs, body, kind)
                if ok:
                    return True
            return False

    async def _try_refresh_once(
        self,
        session: aiohttp.ClientSession,
        url: str,
        extra_headers: Optional[Dict[str, str]] = None,
        body: Optional[Dict[str, str]] = None,
        body_kind: Optional[str] = None,  # "json" | "form" | None
    ) -> bool:
        try:
            headers = dict(self._auth_headers())
            if "Authorization" in headers:
                headers.pop("Authorization", None)  # use api-key auth for refresh
            if extra_headers:
                headers.update(extra_headers)

            if body_kind == "json":
                async with session.post(url, headers=headers, json=body) as r:
                    return await self._handle_refresh_response(r)
            elif body_kind == "form":
                async with session.post(url, headers=headers, data=body) as r:
                    return await self._handle_refresh_response(r)
            else:
                async with session.post(url, headers=headers) as r:
                    return await self._handle_refresh_response(r)
        except Exception:
            return False

    async def _handle_refresh_response(self, r: aiohttp.ClientResponse) -> bool:
        self.last_refresh_status = r.status
        try:
            txt = await r.text()
        except Exception:
            txt = ""
        self.last_refresh_body_snippet = (txt or "")[:200]

        if r.status not in (200, 201):
            return False

        # common token fields
        token = None
        try:
            js = await r.json()
        except Exception:
            js = None
        if isinstance(js, dict):
            for k in ("access_token", "accessToken", "jwt", "token"):
                if k in js and isinstance(js[k], str) and js[k]:
                    token = js[k]
                    break
        if not token:
            # try to extract from raw text if json fails
            token = _extract_token_from_text(txt)

        if not token:
            return False

        await self._reset_session_with_token(token)
        return True

    # ---------- basic calls ----------
    async def ping_user(self) -> bool:
        """Check access token validity; refresh and retry once when 401/403."""
        s = await self._session_get()
        url = f"{self.base_url}/user"
        for attempt in (0, 1):
            try:
                async with s.get(url) as r:
                    self.last_ping_status = r.status
                    try:
                        txt = await r.text()
                    except Exception:
                        txt = ""
                    self.last_ping_body_snippet = (txt or "")[:160]
                    if r.status == 200:
                        return True
                    if r.status in (401, 403) and self._api_key and attempt == 0:
                        if await self.refresh_access_token():
                            s = await self._session_get()  # rebuilt with new token
                            continue
                return False
            except Exception:
                if self._api_key and attempt == 0:
                    if await self.refresh_access_token():
                        s = await self._session_get()
                        continue
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

        for attempt in (0, 1):
            for idx in order:
                path, params = shapes[idx]
                url = f"{self.base_url}{path}"
                try:
                    async with s.get(url, params=params) as r:
                        if r.status in (401, 403) and self._api_key and attempt == 0:
                            if await self.refresh_access_token():
                                s = await self._session_get()
                                break  # restart outer loop after refresh
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


def _extract_token_from_text(txt: str) -> Optional[str]:
    # super light heuristic (JWT-like or long bearer)
    import re
    m = re.search(r'eyJ[0-9A-Za-z_\-]+\.?[0-9A-Za-z_\-]*\.?[0-9A-Za-z_\-]*', txt)
    if m:
        return m.group(0)
    m2 = re.search(r'"(access_token|accessToken|jwt|token)"\s*:\s*"([^"]+)"', txt)
    if m2:
        return m2.group(2)
    return None


class SwyftxExchangeClient:
    """
    Looks like our CCXT ExchangeClient:
      - load(): sets markets_loaded (via token or token+refresh)
      - fetch_tob(): returns dict {'bid','ask','ts'}
      - close(): closes aiohttp
      - symbol_map: configured symbols
      - diagnostics exposed for logs
    """
    def __init__(self, symbols: Iterable[str], access_token: str, api_key: str, demo: bool = False, refresh_url: str = ""):
        self.id = "swyftx"
        base = DEMO_BASE if demo else DEFAULT_BASE
        self.client = SwyftxAsyncClient(
            access_token=access_token,
            api_key=api_key,
            base_url=base,
            refresh_url_override=refresh_url,
        )
        self.symbol_map = set(symbols)
        self.markets_loaded = False
        self.needs_auth = False
        self._last_status = None
        self._last_body = None
        self._last_refresh_status = None
        self._last_refresh_body = None

    async def load(self):
        # If no seed token, try to mint one immediately
        if not self.client._access_token and self.client._api_key:
            await self.client.refresh_access_token()

        ok = await self.client.ping_user()
        self._last_status = self.client.last_ping_status
        self._last_body = self.client.last_ping_body_snippet
        self._last_refresh_status = self.client.last_refresh_status
        self._last_refresh_body = self.client.last_refresh_body_snippet

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
