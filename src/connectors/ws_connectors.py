import asyncio
import json
import websockets
from typing import Callable, Awaitable, Dict, Any

from orderbook.exchange_orderbook_manager import ExchangeOrderBookManager
from orderbook.parsers.independentreserve import parse_ir_event
from connectors.ccxt_rest import CCXTRestConnector

# --- Independent Reserve (IR) ---

_IR_SYMBOL_MAP = {
    # AUD markets: map unified symbol -> IR channel suffix
    "BTC/AUD": "xbt",
    "ETH/AUD": "eth",
    "XRP/AUD": "xrp",
    "LTC/AUD": "ltc",
    "DOGE/AUD": "doge",
    "ADA/AUD": "ada",
    "SOL/AUD": "sol",
    "MATIC/AUD": "matic",
    "LINK/AUD": "link",
    "DOT/AUD": "dot",
    "AVAX/AUD": "avax",
    "SHIB/AUD": "shib",
}

async def _ir_rest_snapshot(symbol: str) -> Dict[str, Any]:
    rest = CCXTRestConnector("independentreserve")
    try:
        snap = await rest.get_orderbook_rest(symbol)
    finally:
        await rest.close()
    return snap

async def subscribe_ir_orderbook(symbol: str, on_orderbook: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
    """
    Subscribes to Independent Reserve orderbook via WS and feeds normalized updates to `on_orderbook`.
    Uses ExchangeOrderBookManager for state, with nonce-based resync to REST snapshot.
    """
    if symbol not in _IR_SYMBOL_MAP:
        raise ValueError(f"Unsupported IR symbol: {symbol}")

    url = "wss://websockets.independentreserve.com"
    channel = f"orderbook-{_IR_SYMBOL_MAP[symbol]}"
    subscribe_message = {"Event": "Subscribe", "Data": [channel]}

    ob = ExchangeOrderBookManager("independentreserve", depth=20)
    ob.parser = parse_ir_event
    ob.resync_callback = lambda: _ir_rest_snapshot(symbol)

    async with websockets.connect(url) as ws:
        # subscribe
        await ws.send(json.dumps(subscribe_message))

        async for raw in ws:
            try:
                data = json.loads(raw)
            except Exception:
                continue

            evt = data.get("Event")

            # Sequence/nonce check (IR uses Nonce)
            seq = data.get("Nonce")
            await ob.check_seq(seq)

            # Parser will no-op for heartbeat/subscriptions
            ob.update_from_event(data)

            # Provide top of book to caller when we have both sides
            best_bid, best_ask = ob.best_bid(), ob.best_ask()
            if best_bid and best_ask:
                await on_orderbook({
                    "bids": ob.top_bids(),
                    "asks": ob.top_asks(),
                    "timestamp": data.get("Time"),
                })
