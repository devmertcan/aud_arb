import asyncio, websockets, json

async def ws_listen(url, subscribe_message, on_msg):
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps(subscribe_message))
        async for msg in ws:
            data = json.loads(msg)
            await on_msg(data)

async def subscribe_ir_orderbook(symbol: str, callback):
    url = "wss://api.independentreserve.com/WebSocket"
    subscribe_message = {
        "type": "subscribe",
        "channel": f"orderbook-{symbol.replace('/', '').lower()}"
    }
    async def on_msg(data):
        # Normalize to your bot’s format
        ob = {
            "bids": data.get("bids", []),
            "asks": data.get("asks", []),
            "timestamp": data.get("timestamp")
        }
        await callback(ob)

    await ws_listen(url, subscribe_message, on_msg)
