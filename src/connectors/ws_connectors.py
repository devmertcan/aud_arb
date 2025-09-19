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

async def subscribe_ir_orderbook(symbol: str, callback=None):
    url = "wss://websockets.independentreserve.com/?subscribe=orderbook-xbt"

    async def on_msg(data):
        event = data.get("Event")
        if event in ["NewOrder"]:
            price = data["Data"]["Price"]["aud"]
            volume = data["Data"]["Volume"]
            if data["Data"]["OrderType"] == "LimitBid":
                ob = {"bids": [(price, volume)], "asks": [], "timestamp": data.get("Time")}
            else:
                ob = {"bids": [], "asks": [(price, volume)], "timestamp": data.get("Time")}
            if callback:
                await callback(ob)

        elif event in ["OrderChanged", "OrderCanceled"]:
            # These don’t include price, just volume changes or removals
            # For now, just ignore them (later you can wire this into OrderbookManager)
            return

        elif event == "Subscriptions":
            print("[IR] Subscribed:", data.get("Data"))

        elif event == "Heartbeat":
            print("[IR] Heartbeat")

    await ws_listen(url, None, on_msg)
