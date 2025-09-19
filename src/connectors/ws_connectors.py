import asyncio
import websockets
import json
import logging
from utils.logging_config import setup_logger

logger = setup_logger("ws_connectors")

async def ws_listen(url, subscribe_message, on_msg):
    async with websockets.connect(url) as ws:
        if subscribe_message:
            await ws.send(json.dumps(subscribe_message))
        async for msg in ws:
            try:
                data = json.loads(msg)
                await on_msg(data)
            except Exception as e:
                logger.error(f"Failed to process WS message: {e}")

async def subscribe_ir_orderbook(symbol: str, callback=None):
    url = "wss://websockets.independentreserve.com/?subscribe=orderbook-xbt"

    async def on_msg(data):
        event = data.get("Event")
        if event == "NewOrder":
            price = data["Data"]["Price"]["aud"]
            volume = data["Data"]["Volume"]
            if data["Data"]["OrderType"] == "LimitBid":
                ob = {"bids": [(price, volume)], "asks": [], "timestamp": data.get("Time")}
            else:
                ob = {"bids": [], "asks": [(price, volume)], "timestamp": data.get("Time")}
            if callback:
                await callback(ob)

        elif event in ["OrderChanged", "OrderCanceled"]:
            return

        elif event == "Subscriptions":
            logger.info(f"[IR] Subscribed: {data.get('Data')}")

        elif event == "Heartbeat":
            logger.info("[IR] Heartbeat")

    await ws_listen(url, None, on_msg)
