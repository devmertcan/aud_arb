import asyncio
import json
import websockets

async def ws_listen(url, subscribe_message, on_msg):
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps(subscribe_message))
        async for msg in ws:
            try:
                data = json.loads(msg)
            except Exception:
                data = msg  # fallback if it’s plain text
            await on_msg(data)

async def subscribe_ir_orderbook(symbol: str, callback):
    url = "wss://websockets.independentreserve.com"

    # This subscribe_message is just a placeholder until we see what IR expects
    subscribe_message = {
        "type": "subscribe",
        "channel": f"orderbook-{symbol.replace('/', '').lower()}"
    }

    async def on_msg(data):
        print("RAW message from IR:", data)  # 👈 print full payload
        await callback({"bids": [], "asks": [], "timestamp": None})

    await ws_listen(url, subscribe_message, on_msg)
