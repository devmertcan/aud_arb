import asyncio
import json
import time
from typing import Dict, Any, List

# NOTE: To keep dependencies light and aligned with Milestone 1,
# we do WS only for exchanges with stable, easy public book feeds (OKX, Kraken).
# Others fall back to REST in ccxt_rest.

try:
    import websockets
except Exception:
    websockets = None  # If websockets not installed, caller should skip WS connectors.

class OKXWS:
    URL = "wss://ws.okx.com:8443/ws/v5/public"

    def __init__(self, exchange_id: str = "okx"):
        self.exchange_id = exchange_id
        self._closed = False

    async def close(self):
        self._closed = True

    def _chan(self, inst_id: str) -> Dict[str, Any]:
        return {"channel": "books5", "instId": inst_id.replace("/", "-")}

    async def stream_pairs(self, pairs: List[str], out_queue: asyncio.Queue):
        if websockets is None:
            return
        sub = {
            "op": "subscribe",
            "args": [self._chan(p) for p in pairs],
        }
        async with websockets.connect(self.URL, ping_interval=20) as ws:
            await ws.send(json.dumps(sub))
            while not self._closed:
                msg = json.loads(await ws.recv())
                if "arg" in msg and "data" in msg:
                    for book in msg["data"]:
                        bids = book.get("bids", [])
                        asks = book.get("asks", [])
                        if not bids or not asks:
                            continue
                        best_bid = float(bids[0][0]); bid_sz = float(bids[0][1])
                        best_ask = float(asks[0][0]); ask_sz = float(asks[0][1])
                        await out_queue.put({
                            "ts_ms": int(time.time() * 1000),
                            "exchange": self.exchange_id,
                            "pair": book["arg"]["instId"].replace("-", "/"),
                            "best_bid": best_bid, "bid_size": bid_sz,
                            "best_ask": best_ask, "ask_size": ask_sz,
                            "source": "WS",
                        })

class KrakenWS:
    URL = "wss://ws.kraken.com/"

    def __init__(self, exchange_id: str = "kraken"):
        self.exchange_id = exchange_id
        self._closed = False

    async def close(self):
        self._closed = True

    def _pair(self, p: str) -> str:
        # Kraken often accepts "BTC/AUD" as-is; adapt here if needed.
        return p

    async def stream_pairs(self, pairs: List[str], out_queue: asyncio.Queue):
        if websockets is None:
            return
        sub = {
            "event": "subscribe",
            "pair": [self._pair(p) for p in pairs],
            "subscription": {"name": "book", "depth": 10}
        }
        async with websockets.connect(self.URL, ping_interval=20) as ws:
            await ws.send(json.dumps(sub))
            chan_map = {}
            while not self._closed:
                raw = await ws.recv()
                msg = json.loads(raw)
                # Kraken sends arrays for book updates; simplify handling:
                if isinstance(msg, list) and len(msg) > 1 and isinstance(msg[1], dict):
                    book = msg[1]
                    pair = msg[-1] if isinstance(msg[-1], str) else None
                    bids = book.get("b", [])
                    asks = book.get("a", [])
                    if bids:
                        best_bid = float(bids[0][0]); bid_sz = float(bids[0][1])
                    else:
                        best_bid = None; bid_sz = 0.0
                    if asks:
                        best_ask = float(asks[0][0]); ask_sz = float(asks[0][1])
                    else:
                        best_ask = None; ask_sz = 0.0
                    if best_bid and best_ask:
                        await out_queue.put({
                            "ts_ms": int(time.time() * 1000),
                            "exchange": self.exchange_id,
                            "pair": pair or "",
                            "best_bid": best_bid, "bid_size": bid_sz,
                            "best_ask": best_ask, "ask_size": ask_sz,
                            "source": "WS",
                        })
                elif isinstance(msg, dict) and msg.get("event") == "subscriptionStatus":
                    # can log/track, omitted for brevity
                    pass
