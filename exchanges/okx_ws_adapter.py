# /opt/aud_arb/exchanges/okx_ws_adapter.py
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
from typing import Dict, Iterable, Optional, Tuple

import aiohttp
from loguru import logger

OKX_PUBLIC_WS = "wss://ws.okx.com:8443/ws/v5/public"
OKX_PRIVATE_WS = "wss://ws.okx.com:8443/ws/v5/private"
OKX_REST_BASE = "https://www.okx.com"


def _okx_sign(secret: str, ts: str, method: str, path: str, body: str = "") -> str:
    prehash = f"{ts}{method}{path}{body}".encode()
    digest = hmac.new(secret.encode(), prehash, hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


class OkxWSClient:
    """
    OKX WebSocket v5 client for order book:
      - Channel: 'books5' (default) or 'books-l2-tbt'
      - Auto-reconnect + re-subscribe
      - Optional private login (kept alive if creds provided)
    """

    def __init__(
        self,
        instruments: Iterable[str],
        channel: str = "books5",
        api_key: str = "",
        api_secret: str = "",
        passphrase: str = "",
        public_ws_url: str = OKX_PUBLIC_WS,
        private_ws_url: str = OKX_PRIVATE_WS,
        ping_interval: float = 20.0,
        connect_timeout: float = 8.0,
        reconnect_delay: float = 3.0,
    ):
        self.public_ws_url = public_ws_url
        self.private_ws_url = private_ws_url
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        self.passphrase = passphrase or ""
        self.channel = channel
        self.inst_ids = list(instruments)

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws_pub: Optional[aiohttp.ClientWebSocketResponse] = None
        self._ws_priv: Optional[aiohttp.ClientWebSocketResponse] = None

        self._connect_timeout = connect_timeout
        self._ping_interval = ping_interval
        self._reconnect_delay = reconnect_delay

        self._stop = asyncio.Event()

        # in-memory best bid/ask store: instId -> (bid, ask, ts)
        self.tob: Dict[str, Tuple[float, float, float]] = {}

        # tasks
        self._task_pub_main: Optional[asyncio.Task] = None
        self._task_priv_main: Optional[asyncio.Task] = None

        # diagnostics
        self._first_tick_logged: set[str] = set()

    async def start(self):
        await self._ensure_session()
        self._task_pub_main = asyncio.create_task(self._public_loop(), name="okx_pub_main")
        if self.api_key and self.api_secret and self.passphrase:
            self._task_priv_main = asyncio.create_task(self._private_loop(), name="okx_priv_main")

    async def close(self):
        self._stop.set()
        try:
            if self._task_pub_main: self._task_pub_main.cancel()
            if self._task_priv_main: self._task_priv_main.cancel()
        except Exception:
            pass

        try:
            if self._ws_pub and not self._ws_pub.closed:
                await self._ws_pub.close()
        except Exception:
            pass
        try:
            if self._ws_priv and not self._ws_priv.closed:
                await self._ws_priv.close()
        except Exception:
            pass
        if self._session and not self._session.closed:
            await self._session.close()

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=25.0),
                headers={"User-Agent": "aud-arb/okx-ws/1.2", "Accept": "application/json"},
            )

    async def _public_loop(self):
        args = [{"channel": self.channel, "instId": inst} for inst in self.inst_ids]
        while not self._stop.is_set():
            try:
                await self._ensure_session()
                self._ws_pub = await self._session.ws_connect(self.public_ws_url, heartbeat=None, timeout=self._connect_timeout)
                await self._ws_pub.send_json({"op": "subscribe", "args": args})
                logger.info(f"[okx-ws] subscribed channel={self.channel} instIds={','.join([a['instId'] for a in args])}")
                reader = asyncio.create_task(self._reader_loop_public(self._ws_pub), name="okx_pub_reader")
                pinger = asyncio.create_task(self._pinger_loop(self._ws_pub), name="okx_pub_pinger")
                done, pending = await asyncio.wait({reader, pinger}, return_when=asyncio.FIRST_COMPLETED)
                for t in pending: t.cancel()
            except Exception as e:
                logger.debug(f"[okx-ws] public loop error: {e}")
            try:
                if self._ws_pub and not self._ws_pub.closed:
                    await self._ws_pub.close()
            except Exception:
                pass
            if self._stop.is_set(): break
            await asyncio.sleep(self._reconnect_delay)

    async def _pinger_loop(self, ws: aiohttp.ClientWebSocketResponse):
        while not self._stop.is_set():
            try:
                if ws.closed:
                    return
                await ws.send_json({"op": "ping"})
            except Exception:
                return
            await asyncio.sleep(self._ping_interval)

    async def _reader_loop_public(self, ws: aiohttp.ClientWebSocketResponse):
        while not self._stop.is_set():
            try:
                msg = await ws.receive(timeout=90.0)
            except Exception:
                break

            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue

            try:
                payload = json.loads(msg.data)
            except Exception:
                continue

            if isinstance(payload, dict):
                if payload.get("event") in ("subscribe", "unsubscribe", "login"):
                    continue
                if payload.get("op") == "pong":
                    continue

                arg = payload.get("arg", {})
                chan = arg.get("channel")
                if chan != self.channel:
                    continue
                instId = arg.get("instId")
                data = payload.get("data") or []
                if not instId or not data:
                    continue

                try:
                    entry = data[0]
                    bids = entry.get("bids") or []
                    asks = entry.get("asks") or []
                    if not bids or not asks:
                        continue
                    bid = float(bids[0][0]); ask = float(asks[0][0])
                    ts = time.time()
                    self.tob[instId] = (bid, ask, ts)
                    if instId not in self._first_tick_logged:
                        self._first_tick_logged.add(instId)
                        logger.info(f"[okx-ws:FIRST] {instId} bid={bid:.8f} ask={ask:.8f}")
                except Exception:
                    continue

    async def _private_loop(self):
        while not self._stop.is_set():
            try:
                await self._ensure_session()
                self._ws_priv = await self._session.ws_connect(self.private_ws_url, heartbeat=None, timeout=self._connect_timeout)
                ts = str(round(time.time()))
                sign = _okx_sign(self.api_secret, ts, "GET", "/users/self/verify")
                login_args = [{"apiKey": self.api_key, "passphrase": self.passphrase, "timestamp": ts, "sign": sign}]
                await self._ws_priv.send_json({"op": "login", "args": login_args})
                reader = asyncio.create_task(self._reader_loop_private(self._ws_priv), name="okx_priv_reader")
                pinger = asyncio.create_task(self._pinger_loop(self._ws_priv), name="okx_priv_pinger")
                done, pending = await asyncio.wait({reader, pinger}, return_when=asyncio.FIRST_COMPLETED)
                for t in pending: t.cancel()
            except Exception as e:
                logger.debug(f"[okx-ws] private loop error: {e}")
            try:
                if self._ws_priv and not self._ws_priv.closed:
                    await self._ws_priv.close()
            except Exception:
                pass
            if self._stop.is_set(): break
            await asyncio.sleep(self._reconnect_delay)

    async def _reader_loop_private(self, ws: aiohttp.ClientWebSocketResponse):
        while not self._stop.is_set():
            try:
                msg = await ws.receive(timeout=90.0)
            except Exception:
                break
            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            # Swallow acks/heartbeats; for M2 add orders/balances subs.

class OkxWSExchangeClient:
    def __init__(
        self,
        symbols: Iterable[str],
        channel: str = "books5",
        api_key: str = "",
        api_secret: str = "",
        passphrase: str = "",
        public_ws_url: str = OKX_PUBLIC_WS,
        private_ws_url: str = OKX_PRIVATE_WS,
        rest_base: str = OKX_REST_BASE,
    ):
        self.id = "okx"
        self._symbols_req = list(symbols)
        self.symbol_map = set()
        self._symbol_to_inst: Dict[str, str] = {}
        self._inst_to_symbol: Dict[str, str] = {}
        self.markets_loaded = False
        self.needs_auth = False

        self._rest_base = rest_base.rstrip("/")
        self._client = OkxWSClient(
            instruments=[],
            channel=channel,
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            public_ws_url=public_ws_url,
            private_ws_url=private_ws_url,
        )

    async def load(self):
        instruments = await self._fetch_spot_instruments()
        if not instruments:
            self.markets_loaded = False
            return

        inst_ids = {it.get("instId"): it for it in instruments if it.get("instId")}
        for sym in self._symbols_req:
            try:
                base, quote = sym.split("/")
            except ValueError:
                continue
            inst = f"{base}-{quote}"
            if inst in inst_ids:
                self._symbol_to_inst[sym] = inst
                self._inst_to_symbol[inst] = sym
                self.symbol_map.add(sym)

        self.markets_loaded = True
        if self.symbol_map:
            self._client.inst_ids = list(self._inst_to_symbol.keys())
            await self._client.start()

    async def _fetch_spot_instruments(self):
        url = f"{self._rest_base}/api/v5/public/instruments?instType=SPOT"
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15.0),
                headers={"User-Agent": "aud-arb/okx-rest/1.0", "Accept": "application/json"},
            ) as s:
                async with s.get(url) as r:
                    if r.status != 200:
                        return None
                    js = await r.json()
                    return js.get("data") or []
        except Exception:
            return None

    async def fetch_tob(self, symbol: str):
        inst = self._symbol_to_inst.get(symbol)
        if not inst:
            return None
        tup = self._client.tob.get(inst)
        if not tup:
            return None
        bid, ask, ts = tup
        return {"bid": float(bid), "ask": float(ask), "ts": ts}

    async def close(self):
        await self._client.close()
