import asyncio
import websockets
import json


async def ws_listen(url, subscribe_message, on_msg):
    async with websockets.connect(url) as ws:
        if subscribe_message:
            await ws.send(json.dumps(subscribe_message))
        async for msg in ws:
            data = json.loads(msg)
            await on_msg(data)


async def subscribe_ir_orderbook(symbol: str, callback):
    url = f"wss://websockets.independentreserve.com/?subscribe=orderbook-xbt"
    # NOTE: IR uses XBT not BTC for Bitcoin pairs
    async def on_msg(data):
        if data.get("Event") in ["NewOrder", "OrderChanged", "OrderCanceled"]:
            # Simplified normalized orderbook (partial)
            ob = {
                "bids": [(data["Data"]["Price"]["aud"], data["Data"]["Volume"])]
                if data["Data"]["OrderType"] == "LimitBid" else [],
                "asks": [(data["Data"]["Price"]["aud"], data["Data"]["Volume"])]
                if data["Data"]["OrderType"] == "LimitOffer" else [],
                "timestamp": data.get("Time"),
            }
            await callback(ob)
    await ws_listen(url, None, on_msg)
